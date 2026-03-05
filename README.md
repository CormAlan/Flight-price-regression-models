# Flight price regression models

## Datainhämtning
Filerna `serpapi_flights_scan.py` och `hubs_builder.py`, samt deras körskript `run_13day_sampling.ps1`, används endast för datainsamling och anropar SerpAPI för att hämta flygdata för det kommande året. Du får gärna använda dem också. Körscripten väljer dagar med 13 dagars mellanrum och anropar API:et för alla valda toppflyplatser under de dagarna. Jag valde att scanna efter flyg från ARN till asiens topp 9 flygplatser med 14 dagars mellanrum. Det här var ett misstag eftersom 7 är kongruent med 14 och alla skanningar blev på samma veckodag. Därmed kunde jag inte använda veckodagar som en feature.

## Regressioner
Jag har en mindre fil med en linjär regression (som inte alls gav en så bar korrelation) och en fil med Random Forest (som faktiskt fungerade).

Vår enda beroende variabel är **priset** på flyget, allt annat är features:
- distance_km
- duration_min
- stops
- weekday
- days until departure
## Train/test split 
Man gömmer lite data från modellen för att testa prestanda
```python
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    random_state=42
)
```
(80% till träning, 20% för testning)
### MEA för att evaluera
Mean absolute error:
$$
MAE = \frac{1}{n}\sum |y - \hat{y}|
$$
```python
from sklearn.metrics import mean_absolute_error

mae = mean_absolute_error(y_test, predictions)

print("Mean Absolute Error:", mae)
```
MEA säger hur långt ifrån rätt svar modellen är (MAE=45 $\iff$ modellen snittar ett felvärde på 45 euro).
# Linjär regression
```python
from sklearn.linear_model import LinearRegression

model = LinearRegression()

model.fit(X_train, y_train)
```
### Förutspå priser m.h.a modellen
```python
predictions = model.predict(X_test)
```
Kan outputta:
```
Predicted price: 520
Actual price:    490
```
# Random forest regression
```python
from sklearn.ensemble import RandomForestRegressor

model = RandomForestRegressor(
    n_estimators=200,
    max_depth=10
)

model.fit(X_train, y_train)
```
Den här modellen fångar icke-linjära förhållanden som spikes, rutter, vad ett stop gör
### Kolla vad den har lärt sig
Linjär:
```python
coefficients = pd.DataFrame({
    "Feature": features,
    "Weight": model.coef_
})

print(coefficients)
```
Kommer att skriva ut en tabell med vikten av varje feature
Random forest (använder importance istället för coefficients):


### Visualisering med [[Matplotlib]]
```python
import matplotlib.pyplot as plt

plt.scatter(y_test, predictions)
plt.xlabel("Actual price")
plt.ylabel("Predicted price")
plt.title("Prediction vs Reality")
plt.show()
```
![Prediction vs reality med idell linje och 10 bästa dealsen inkluderade (Random forest)](plots/plot.png)

## Topp 10 bästa deals
