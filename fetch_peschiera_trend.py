import requests
import json

# Peschiera Borromeo - ISTAT code 8084 (Milano province)
url = "https://www.immobiliare.it/api-next/charts/price-chart/?idCategoria=1&idContratto=1&idRegione=lom&idProvincia=MI&idComune=8062&__lang=it"

r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)

if r.ok:
    data = r.json()
    
    print("=" * 80)
    print("PESCHIERA BORROMEO - Historical Price Trend")
    print("=" * 80)
    print()
    print(f"Location: {data.get('subtitle')} - {data.get('title')}")
    print(f"Unit: {data.get('price-meters')}")
    print()
    
    labels = data['labels']
    values = [float(v) for v in data['values']]
    
    print(f"Data points: {len(labels)} months")
    print(f"Date range: {labels[0]} to {labels[-1]}")
    print()
    print(f"Latest price (Feb 2026): €{values[-1]:,.0f}")
    print(f"Oldest price (Nov 2017): €{values[0]:,.0f}")
    print(f"Min price historic: €{min(values):,.0f}")
    print(f"Max price historic: €{max(values):,.0f}")
    change_pct = (values[-1] / values[0] - 1) * 100
    print(f"Change (Nov 2017 → Feb 2026): {change_pct:+.1f}%")
    print()
    
    print("=" * 80)
    print("Year-by-Year Comparison")
    print("=" * 80)
    
    years_data = {}
    for label, val in zip(labels, values):
        year = label.split('-')[0]
        if year not in years_data:
            years_data[year] = []
        years_data[year].append(val)
    
    for year in sorted(years_data.keys()):
        vals = years_data[year]
        avg = sum(vals) / len(vals)
        min_y = min(vals)
        max_y = max(vals)
        print(f"{year}: €{avg:>7,.0f}/m²  (min: €{min_y:>7,.0f}, max: €{max_y:>7,.0f})")
    
    print()
    print("=" * 80)
    print("3-Year Comparison: 2023 vs 2024 vs 2025")
    print("=" * 80)
    
    for year in ['2023', '2024', '2025']:
        if year in years_data:
            vals = years_data[year]
            avg = sum(vals) / len(vals)
            print(f"{year}: €{avg:,.0f}/m²")
    
else:
    print(f"Error: {r.status_code}")
