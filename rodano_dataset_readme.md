
# Rodano Real Estate Quantitative Dataset

Files included:

## rodano_residential_transactions.csv
Columns:
- date: transaction month (YYYY-MM)
- category: A02, A03, A07
- surface_m2: reported residential surface
- price_total_eur: declared transaction value
- garage_included: boolean (garage bundled in sale)
- price_per_m2: derived metric

## rodano_garage_transactions.csv
Columns:
- date
- category (C06)
- surface_m2
- price_total_eur
- price_per_m2

## Suggested analysis

Examples:

Python (pandas):

```python
import pandas as pd

res = pd.read_csv("rodano_residential_transactions.csv")
gar = pd.read_csv("rodano_garage_transactions.csv")

res.groupby("category")["price_per_m2"].mean()
res.groupby(res["date"].str[:4])["price_per_m2"].mean()
```

Possible models:
- price trend regression
- category dummy regression
- hedonic pricing model
