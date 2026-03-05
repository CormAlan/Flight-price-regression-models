import pandas as pd
from sklearn.linear_model import LinearRegression
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


# LINJÄR REGRESSION
model = LinearRegression()
model.fit(X_train, y_train)

# PREDICTION
predictions = model.predict(X_test)

# MEA evaluering
mae = mean_absolute_error(y_test, predictions)
print("Mean Absolute Error:", mae)

# KOEFFICIENTER
coefficients = pd.DataFrame({
    "Feature": features,
    "Weight": model.coef_
})

print(coefficients)

# MATPLOTLIB
import matplotlib.pyplot as plt

plt.scatter(y_test, predictions)
plt.xlabel("Actual price")
plt.ylabel("Predicted price")
plt.title("Prediction vs Reality")
plt.show()