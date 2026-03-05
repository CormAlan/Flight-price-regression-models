import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

df = pd.read_csv("flights_dataset.csv")

df["departure_date"] = pd.to_datetime(df["departure_date"])
df["weekday"] = df["departure_date"].dt.weekday # For weekday, Monday=0, Sunday=6
today = pd.Timestamp.today()
df["days_until_departure"] = (df["departure_date"] - today).dt.days # Days until departure, can be negative if the flight has already departed

df["distance_per_stop"] = df["distance_km"] / (df["stops"] + 1) # Adding 1 to stops to avoid division by zero for non-stop flights. This gives us the average distance per individual flight segment.

features = ["distance_km",
            "duration_min",
            "stops",
            "weekday",
            "days_until_departure",
            "distance_per_stop"
            ]

x = df[features]
y = df["price"] 

# TRAIN-TEST SPLIT
X_train, X_test, y_train, y_test = train_test_split(
    x, y,
    test_size=0.2,
    random_state=42
)

# RANDOM FOREST REGRESSION
model = RandomForestRegressor(n_estimators=100, random_state=42, max_depth=10)
model.fit(X_train, y_train)

# PREDICTION
predictions = model.predict(X_test)

# MEA evaluering
mae = mean_absolute_error(y_test, predictions)
print("Mean Absolute Error:", mae)

# KOEFFICIENTER
importances = pd.DataFrame({
    "Feature": features,
    "Importance": model.feature_importances_
})

print(importances.sort_values(by="Importance", ascending=False))

# MATPLOTLIB
import matplotlib.pyplot as plt

# Prediction vs Reality plot (test set)
plt.scatter(y_test, predictions, alpha=0.35)
plt.xlabel("Actual price")
plt.ylabel("Predicted price")
plt.title("Prediction vs Reality (test set)")
plt.show()

# Flyg deal detektion
df["predicted_price"] = model.predict(x)
df["price_difference"] = df["predicted_price"] - df["price"]
print(df.sort_values("price_difference", ascending=False).head(20))

# --- Minimal add-on: label top deals on Actual vs Predicted (ALL rows) ---
top_n = 10
top_deals = df.nlargest(top_n, "price_difference")

plt.figure()
plt.scatter(df["price"], df["predicted_price"], alpha=0.35)

# y = x (perfekt prediction) linje
lo = min(df["price"].min(), df["predicted_price"].min())
hi = max(df["price"].max(), df["predicted_price"].max())
plt.plot([lo, hi], [lo, hi])

plt.xlabel("Actual price")
plt.ylabel("Predicted price")
plt.title("Actual vs Predicted (top deals labeled)")

for _, row in top_deals.iterrows():
    a = float(row["price"])
    p = float(row["predicted_price"])
    label = f'+{row["price_difference"]:.0f}'
    plt.annotate(label, (a, p), xytext=(5, 5), textcoords="offset points", fontsize=8)

plt.show()