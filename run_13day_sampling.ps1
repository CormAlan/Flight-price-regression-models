param(
  [string]$Origin = "ARN",
  [string]$Continent = "AS",
  [string]$StartDate = (Get-Date).AddDays(10).ToString("yyyy-MM-dd"),
  [string]$EndDate   = (Get-Date).AddMonths(12).ToString("yyyy-MM-dd"),
  [int]$StepDays = 13,
  [int]$TopPerContinent = 9,
  [int]$Limit = 10,
  [string]$Currency = "EUR",
  [string]$OutCsv = "flights_dataset.csv",
  [string]$TempDir = "runs",
  [switch]$Verbose
)

# Ensure temp dir exists
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null

# Helper: ISO date parse
$start = [DateTime]::ParseExact($StartDate, "yyyy-MM-dd", $null)
$end   = [DateTime]::ParseExact($EndDate,   "yyyy-MM-dd", $null)

# We'll write combined CSV with one header
if (Test-Path $OutCsv) { Remove-Item $OutCsv -Force }

$first = $true
$d = $start
$runIndex = 0

while ($d -le $end) {
  $runIndex++
  $dateFrom = $d.ToString("yyyy-MM-dd")
  $dateTo   = $d.ToString("yyyy-MM-dd")   # single-day query per run (cleanest)
  $tmpCsv   = Join-Path $TempDir ("run_{0:0000}_{1}.csv" -f $runIndex, $dateFrom)

  Write-Host ("[run {0}] {1} -> {2}  (top={3}, limit={4})" -f $runIndex, $dateFrom, $dateTo, $TopPerContinent, $Limit)

  $args = @(
    "serpapi_flights_scan.py",
    "--origin", $Origin,
    "--continent", $Continent,
    "--date-range", "$dateFrom`:$dateTo",
    "--top-per-continent", "$TopPerContinent",
    "--limit", "$Limit",
    "--top", "999999",
    "--csv-out", $tmpCsv,
    "--currency", $Currency
  )
  if ($Verbose) { $args += "--verbose" }

  python @args
  if ($LASTEXITCODE -ne 0) {
    Write-Warning "Python run failed for $dateFrom (exit $LASTEXITCODE). Skipping concat for this run."
    $d = $d.AddDays($StepDays)
    continue
  }

  if (!(Test-Path $tmpCsv)) {
    Write-Warning "No CSV produced for $dateFrom. Skipping."
    $d = $d.AddDays($StepDays)
    continue
  }

  # Concatenate: keep header only from first file
  if ($first) {
    Get-Content $tmpCsv | Set-Content $OutCsv
    $first = $false
  } else {
    Get-Content $tmpCsv | Select-Object -Skip 1 | Add-Content $OutCsv
  }

  $d = $d.AddDays($StepDays)
}

Write-Host "Done. Combined CSV: $OutCsv"