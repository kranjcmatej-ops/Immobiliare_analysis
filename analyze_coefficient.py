import pandas as pd

# Load full ML dataset (all years)
df = pd.read_csv('rodano_real_estate_ml_dataset.csv')

print("Looking at historical data around 103 m² and 124 m²:\n")

# Properties around 103 m²
df_103 = df[(df['surface_m2'] >= 100) & (df['surface_m2'] <= 106)]
print(f"Properties 100-106 m² ({len(df_103)} listings):")
print(df_103[['date', 'surface_m2', 'price_per_m2', 'price_total_eur']].to_string(index=False))

print(f"\nAverage price/m² (100-106 m²): EUR {df_103['price_per_m2'].mean():.2f}")
print(f"Median price/m² (100-106 m²): EUR {df_103['price_per_m2'].median():.2f}")

# Properties around 124 m²
df_124 = df[(df['surface_m2'] >= 120) & (df['surface_m2'] <= 130)]
print(f"\n\nProperties 120-130 m² ({len(df_124)} listings):")
print(df_124[['date', 'surface_m2', 'price_per_m2', 'price_total_eur']].to_string(index=False))

print(f"\nAverage price/m² (120-130 m²): EUR {df_124['price_per_m2'].mean():.2f}")
print(f"Median price/m² (120-130 m²): EUR {df_124['price_per_m2'].median():.2f}")

# Calculate coefficient
avg_103 = df_103['price_per_m2'].mean()
avg_124 = df_124['price_per_m2'].mean()

if avg_103 > 0:
    coeff = (avg_124 - avg_103) / avg_103 * 100
    ratio = avg_124 / avg_103
    print(f"\n\n📊 COEFFICIENT ANALYSIS:")
    print(f"   Average 103 m² range: EUR {avg_103:.2f}/m²")
    print(f"   Average 124 m² range: EUR {avg_124:.2f}/m²")
    print(f"   Difference: EUR {avg_124 - avg_103:.2f}/m²")
    print(f"   Percentage change: {coeff:.2f}%")
    print(f"   Ratio (124m²/103m²): {ratio:.4f}")
