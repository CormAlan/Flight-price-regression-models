import argparse
import asyncio
import csv
import datetime as dt
import hashlib
import json
import math
import os
import random
import sqlite3
import time
import urllib.request
import re

import requests

OURAIRPORTS_AIRPORTS_CSV = "https://ourairports.com/airports.csv"
OPENFLIGHTS_AIRPORTS_DAT = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
OPENFLIGHTS_ROUTES_DAT = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"

CONTINENT_NAME = {
    "AF": "africa",
    "AS": "asia",
    "EU": "europe",
    "NA": "NA",
    "SA": "SA",
    "OC": "oceania",
    "AN": "antarctica",
}

TYPE_WEIGHT = {
    "large_airport": 1000,
    "medium_airport": 200,
    "small_airport": 0,
}

CONTINENT_IATA_OVERRIDE = {
    "IST": "EU",
    "SAW": "EU",
}

def download(url, path, verbose=False):
    if verbose:
        print("[download]", url, "->", path, flush=True)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with urllib.request.urlopen(url) as r, open(path, "wb") as f:
        f.write(r.read())

def parse_date_range(s):
    a, b = s.split(":")
    start = dt.date.fromisoformat(a)
    end = dt.date.fromisoformat(b)
    if end < start:
        raise ValueError("bad date-range")
    out = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return out

def haversine_km(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def read_iata_file(path):
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip().upper()
            if not s:
                continue
            if s.startswith("#"):
                continue
            out.append(s)
    return out

def read_ourairports_airports_csv(path):
    out = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            iata = (row.get("iata_code") or "").strip().upper()
            if not iata:
                continue
            lat = row.get("latitude_deg")
            lon = row.get("longitude_deg")
            try:
                lat = float(lat) if lat not in (None, "") else None
            except Exception:
                lat = None
            try:
                lon = float(lon) if lon not in (None, "") else None
            except Exception:
                lon = None
            out[iata] = {
                "iata": iata,
                "name": (row.get("name") or "").strip(),
                "continent": (row.get("continent") or "").strip().upper(),
                "iso_country": (row.get("iso_country") or "").strip().upper(),
                "airport_type": (row.get("type") or "").strip(),
                "scheduled_service": (row.get("scheduled_service") or "").strip().lower(),
                "lat": lat,
                "lon": lon,
            }
    return out

def read_openflights_airports_dat(path):
    # openflights airports.dat is CSV-ish with quotes, variable commas inside quotes
    # we just want iata, lat, lon, name
    out = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = list(csv.reader([line]))[0]
            if len(parts) < 8:
                continue
            iata = (parts[4] or "").strip().upper()
            if len(iata) != 3:
                continue
            try:
                lat = float(parts[6])
                lon = float(parts[7])
            except Exception:
                lat, lon = None, None
            out[iata] = {"iata": iata, "name": parts[1], "lat": lat, "lon": lon}
    return out

def read_openflights_routes_degree(path):
    # degree on IATA graph (count both incoming/outgoing)
    deg = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = list(csv.reader([line]))[0]
            if len(parts) < 5:
                continue
            src = (parts[2] or "").strip().upper()
            dst = (parts[4] or "").strip().upper()
            if len(src) == 3:
                deg[src] = deg.get(src, 0) + 1
            if len(dst) == 3:
                deg[dst] = deg.get(dst, 0) + 1
    return deg

def build_destinations_for_continent(continent, data_dir, top_n, min_degree, include_medium, no_download, export_path, verbose):
    continent = continent.strip().upper()
    os.makedirs(data_dir, exist_ok=True)

    our_path = os.path.join(data_dir, "ourairports_airports.csv")
    of_air_path = os.path.join(data_dir, "openflights_airports.dat")
    of_routes_path = os.path.join(data_dir, "openflights_routes.dat")

    if not no_download and not os.path.exists(our_path):
        download(OURAIRPORTS_AIRPORTS_CSV, our_path, verbose=verbose)
    if not no_download and not os.path.exists(of_air_path):
        download(OPENFLIGHTS_AIRPORTS_DAT, of_air_path, verbose=verbose)
    if not no_download and not os.path.exists(of_routes_path):
        download(OPENFLIGHTS_ROUTES_DAT, of_routes_path, verbose=verbose)

    if not os.path.exists(our_path) or not os.path.exists(of_air_path) or not os.path.exists(of_routes_path):
        raise FileNotFoundError("missing data files in data-dir")

    our = read_ourairports_airports_csv(our_path)
    deg = read_openflights_routes_degree(of_routes_path)

    candidates = []
    for iata, row in our.items():
        c = CONTINENT_IATA_OVERRIDE.get(iata, row.get("continent", ""))
        if c != continent:
            continue
        if row.get("scheduled_service") != "yes":
            continue
        t = row.get("airport_type")
        if t == "small_airport":
            continue
        if (not include_medium) and t != "large_airport":
            continue
        d = deg.get(iata, 0)
        if d < min_degree:
            continue
        score = d + TYPE_WEIGHT.get(t, 0)
        candidates.append((score, d, iata))

    candidates.sort(reverse=True)
    picked = [iata for _, _, iata in candidates[:top_n]]

    if export_path:
        os.makedirs(os.path.dirname(export_path) or ".", exist_ok=True)
        with open(export_path, "w", encoding="utf-8") as f:
            for iata in picked:
                f.write(iata + "\n")
        if verbose:
            print("[hubs] wrote", export_path, flush=True)

    return picked

def _parse_price(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        s = s.replace("\xa0", " ")
        digits = []
        for ch in s:
            if ch.isdigit() or ch in ",.":
                digits.append(ch)
        if not digits:
            return None
        s2 = "".join(digits)
        # handle "1,234.56" and "1.234,56"
        if s2.count(",") and s2.count("."):
            if s2.rfind(",") > s2.rfind("."):
                s2 = s2.replace(".", "").replace(",", ".")
            else:
                s2 = s2.replace(",", "")
        else:
            # if only comma, treat as decimal if looks like cents
            if s2.count(",") == 1 and s2.count(".") == 0:
                a, b = s2.split(",")
                if len(b) in (1, 2):
                    s2 = a + "." + b
                else:
                    s2 = s2.replace(",", "")
        try:
            return float(s2)
        except Exception:
            return None
    return None

def _duration_minutes(item):
    x = item.get("total_duration")
    if isinstance(x, (int, float)):
        return int(x)
    if isinstance(x, str):
        s = x.lower()
        mins = 0
        # formats like "12 hr 35 min"
        for part in s.replace("hours", "hr").replace("hour", "hr").replace("mins", "min").split():
            pass
        # quick parse
        nums = re.findall(r"(\d+)\s*hr", s)
        if nums:
            mins += 60 * int(nums[0])
        nums = re.findall(r"(\d+)\s*min", s)
        if nums:
            mins += int(nums[0])
        return mins if mins > 0 else None
    return None

def _count_stops(item):
    flights = item.get("flights")
    if not isinstance(flights, list):
        return None
    if len(flights) <= 1:
        return 0
    return len(flights) - 1

class Cache:
    def __init__(self, path):
        self.path = path
        self._init()

    def _init(self):
        con = sqlite3.connect(self.path)
        try:
            cur = con.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS cache (k TEXT PRIMARY KEY, v TEXT NOT NULL, ts REAL NOT NULL)"
            )
            con.commit()
        finally:
            con.close()

    def get(self, k, ttl_seconds):
        con = sqlite3.connect(self.path)
        try:
            cur = con.cursor()
            cur.execute("SELECT v, ts FROM cache WHERE k=?", (k,))
            row = cur.fetchone()
            if not row:
                return None
            v, ts = row
            if (time.time() - ts) > ttl_seconds:
                return None
            return v
        finally:
            con.close()

    def set(self, k, v):
        con = sqlite3.connect(self.path)
        try:
            cur = con.cursor()
            cur.execute("INSERT OR REPLACE INTO cache (k,v,ts) VALUES (?,?,?)", (k, v, time.time()))
            con.commit()
        finally:
            con.close()

class RateLimit:
    def __init__(self, rps, burst):
        self.rps = float(rps)
        self.burst = int(burst)
        self.tokens = float(burst)
        self.last = time.time()
        self.lock = asyncio.Lock()

    async def take(self):
        while True:
            async with self.lock:
                now = time.time()
                dt_ = now - self.last
                self.last = now
                self.tokens = min(self.burst, self.tokens + dt_ * self.rps)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                need = (1.0 - self.tokens) / self.rps if self.rps > 0 else 1.0
            await asyncio.sleep(max(0.01, need))

def _cache_key(origin, dest, departure_date, currency, hl, limit):
    s = json.dumps(
        {
            "engine": "google_flights",
            "type": 2,
            "origin": origin,
            "dest": dest,
            "date": departure_date,
            "currency": currency,
            "hl": hl,
            "limit": limit,
        },
        sort_keys=True,
    )
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def normalize_hits(origin, dest, departure_date, payload, our_by_iata, currency):
    out = []
    oa = our_by_iata.get(origin)
    da = our_by_iata.get(dest)

    dist_km = None
    if oa and da:
        dist_km = haversine_km(oa.get("lat"), oa.get("lon"), da.get("lat"), da.get("lon"))

    def add_from_list(lst, bucket):
        if not isinstance(lst, list):
            return
        for item in lst:
            if not isinstance(item, dict):
                continue
            price = _parse_price(item.get("price"))
            if price is None:
                continue
            duration = _duration_minutes(item)
            stops = _count_stops(item)
            row = {
                "origin": origin,
                "dest": dest,
                "departure_date": departure_date,
                "price": price,
                "currency": currency,
                "duration_min": duration,
                "stops": stops,
                "source_bucket": bucket,
                "distance_km": dist_km,
                "origin_lat": oa.get("lat") if oa else None,
                "origin_lon": oa.get("lon") if oa else None,
                "dest_lat": da.get("lat") if da else None,
                "dest_lon": da.get("lon") if da else None,
                "origin_name": oa.get("name") if oa else None,
                "dest_name": da.get("name") if da else None,
                "origin_country": oa.get("iso_country") if oa else None,
                "dest_country": da.get("iso_country") if da else None,
                "raw_json": json.dumps(item, ensure_ascii=False),
            }
            if dist_km and dist_km > 0:
                row["price_per_km"] = price / dist_km
            else:
                row["price_per_km"] = None
            out.append(row)

    add_from_list(payload.get("best_flights"), "best_flights")
    add_from_list(payload.get("other_flights"), "other_flights")
    return out

async def fetch(origin, dest, departure_date, api_key, currency, hl, limiter, cache, ttl_seconds, verbose, per_search_limit):
    key = _cache_key(origin, dest, departure_date, currency, hl, per_search_limit)
    if cache:
        cached = cache.get(key, ttl_seconds)
        if cached:
            if verbose:
                print("[cache] hit", origin, "->", dest, departure_date, flush=True)
            return json.loads(cached)

    await limiter.take()

    params = {
        "engine": "google_flights",
        "departure_id": origin,
        "arrival_id": dest,
        "outbound_date": departure_date,
        "type": 2,
        "currency": currency,
        "hl": hl,
        "api_key": api_key,
    }

    if verbose:
        print("[serpapi] GET", origin, "->", dest, departure_date, flush=True)

    def do_req():
        r = requests.get(SERPAPI_ENDPOINT, params=params, timeout=90)
        r.raise_for_status()
        return r.json()

    payload = await asyncio.to_thread(do_req)

    if cache:
        cache.set(key, json.dumps(payload))

    return payload

async def worker(name, queue, limiter, cache, ttl_seconds, results, our_by_iata, currency, verbose, max_attempts, per_search_limit, api_key, hl):
    while True:
        job = await queue.get()
        if job is None:
            queue.task_done()
            return
        origin, dest, departure_date = job
        ok = False
        attempt = 0
        while not ok and attempt < max_attempts:
            attempt += 1
            try:
                payload = await fetch(
                    origin, dest, departure_date,
                    api_key=api_key,
                    currency=currency,
                    hl=hl,
                    limiter=limiter,
                    cache=cache,
                    ttl_seconds=ttl_seconds,
                    verbose=verbose,
                    per_search_limit=per_search_limit,
                )
                rows = normalize_hits(origin, dest, departure_date, payload, our_by_iata, currency)
                if per_search_limit and per_search_limit > 0:
                    rows.sort(key=lambda r: (r.get("price") is None, r.get("price")))
                    rows = rows[:per_search_limit]
                results.extend(rows)
                ok = True
            except requests.HTTPError as e:
                code = getattr(e.response, "status_code", None)
                if verbose:
                    print("[warn]", name, "http", code, "attempt", attempt, origin, dest, departure_date, flush=True)
                if code == 429:
                    await asyncio.sleep(2.0 + random.random() * 2.0)
                else:
                    await asyncio.sleep(1.0)
            except Exception as e:
                if verbose:
                    print("[warn]", name, "err", str(e)[:200], "attempt", attempt, origin, dest, departure_date, flush=True)
                await asyncio.sleep(1.0)
        queue.task_done()

def write_csv(path, rows):
    if not rows:
        return
    cols = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def build_arg_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--serpapi-api-key", type=str, help="SerpAPI key (or env SERPAPI_API_KEY)")
    p.add_argument("--currency", type=str, default="EUR")
    p.add_argument("--hl", type=str, default="en")
    p.add_argument("--origin", type=str, required=True)
    p.add_argument("--date", type=str)
    p.add_argument("--date-range", type=str)
    p.add_argument("--continent", type=str, choices=list(CONTINENT_NAME.keys()))
    p.add_argument("--destinations", type=str)
    p.add_argument("--destinations-file", type=str)
    p.add_argument("--data-dir", type=str, default="data")
    p.add_argument("--top-per-continent", type=int, default=220)
    p.add_argument("--min-degree", type=int, default=15)
    p.add_argument("--include-medium", action="store_true")
    p.add_argument("--no-download", action="store_true")
    p.add_argument("--export-hubs", type=str)
    p.add_argument("--no-search", action="store_true")
    p.add_argument("--limit", type=int, default=5)
    p.add_argument("--top", type=int, default=200)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--rps", type=float, default=0.5)
    p.add_argument("--burst", type=int, default=1)
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument("--cache-db", type=str, default="cache_serpapi.sqlite")
    p.add_argument("--ttl-hours", type=float, default=24.0)
    p.add_argument("--csv-out", type=str)
    p.add_argument("--json-out", type=str)
    p.add_argument("--verbose", action="store_true")
    return p

async def async_main(args):
    api_key = args.serpapi_api_key or os.environ.get("SERPAPI_API_KEY")
    if not api_key and not args.no_search:
        raise RuntimeError("Missing SerpAPI key")

    if args.date and args.date_range:
        raise ValueError("Use only --date or --date-range")
    if args.date:
        departure_dates = [args.date]
    elif args.date_range:
        departure_dates = parse_date_range(args.date_range)
    else:
        raise ValueError("Provide --date or --date-range")

    origin = args.origin.strip().upper()

    os.makedirs(args.data_dir, exist_ok=True)
    our_path = os.path.join(args.data_dir, "ourairports_airports.csv")
    if not args.no_download and not os.path.exists(our_path):
        download(OURAIRPORTS_AIRPORTS_CSV, our_path, verbose=args.verbose)
    if not os.path.exists(our_path):
        raise FileNotFoundError("missing ourairports_airports.csv")

    our_by_iata = read_ourairports_airports_csv(our_path)

    destinations = []
    if args.destinations:
        destinations = [x.strip().upper() for x in args.destinations.split(",") if x.strip()]
    elif args.destinations_file:
        destinations = read_iata_file(args.destinations_file)
    elif args.continent:
        export_path = None
        if args.export_hubs:
            export_path = os.path.join(args.export_hubs, f"{CONTINENT_NAME[args.continent.upper()]}_hubs.txt")
        destinations = build_destinations_for_continent(
            continent=args.continent,
            data_dir=args.data_dir,
            top_n=args.top_per_continent,
            min_degree=args.min_degree,
            include_medium=args.include_medium,
            no_download=args.no_download,
            export_path=export_path,
            verbose=args.verbose,
        )
    else:
        raise ValueError("Provide --continent or --destinations or --destinations-file")

    jobs = []
    for dest in destinations:
        if dest == origin:
            continue
        for d in departure_dates:
            jobs.append((origin, dest, d))

    if args.verbose:
        mode = "continent" if args.continent else "destinations"
        print("[main] mode=" + mode, "destinations=" + str(len(destinations)), "departure_dates=" + str(len(departure_dates)), "jobs=" + str(len(jobs)), flush=True)
        print("[main] workers=" + str(args.workers), "rps=" + str(args.rps), "burst=" + str(args.burst), "cache=on" if args.cache_db else "cache=off", "ttl_hours=" + str(args.ttl_hours), flush=True)

    if args.no_search:
        return 0

    ttl_seconds = int(float(args.ttl_hours) * 3600.0)
    cache = Cache(args.cache_db) if args.cache_db else None
    limiter = RateLimit(args.rps, args.burst)

    queue = asyncio.Queue()
    results = []

    for job in jobs:
        await queue.put(job)
    for _ in range(args.workers):
        await queue.put(None)

    tasks = []
    for i in range(args.workers):
        tasks.append(asyncio.create_task(worker(
            name="w" + str(i),
            queue=queue,
            limiter=limiter,
            cache=cache,
            ttl_seconds=ttl_seconds,
            results=results,
            our_by_iata=our_by_iata,
            currency=args.currency,
            verbose=args.verbose,
            max_attempts=args.max_attempts,
            per_search_limit=args.limit,
            api_key=api_key,
            hl=args.hl,
        )))

    await queue.join()
    for t in tasks:
        await t

    results.sort(key=lambda r: (r.get("price") is None, r.get("price")))
    if args.top and args.top > 0:
        results = results[:args.top]

    if args.csv_out:
        write_csv(args.csv_out, results)
        if args.verbose:
            print("Wrote CSV:", args.csv_out, flush=True)
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        if args.verbose:
            print("Wrote JSON:", args.json_out, flush=True)

    print("\n=== TOP RESULTS ===\n", flush=True)
    for r in results[:min(20, len(results))]:
        print(r.get("departure_date"), origin, "->", r.get("dest"), "price=", r.get("price"), args.currency, "duration_min=", r.get("duration_min"), "stops=", r.get("stops"), flush=True)

    return 0

def main():
    args = build_arg_parser().parse_args()
    try:
        raise SystemExit(asyncio.run(async_main(args)))
    except KeyboardInterrupt:
        print("\nInterrupted.", flush=True)
        raise SystemExit(130)

if __name__ == "__main__":
    main()
