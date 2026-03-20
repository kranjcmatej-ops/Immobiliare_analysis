import requests
import json

url = "https://www.immobiliare.it/api-next/charts/price-chart/?idCategoria=1&idContratto=1&idRegione=lom&idProvincia=MI&idComune=8075&__lang=it"
headers = {'User-Agent': 'Mozilla/5.0'}

r = requests.get(url, headers=headers, timeout=15)
data = r.json()

print("=== API RESPONSE ===")
print(f"Top-level keys: {list(data.keys())}")
print()

print("=== Non-array fields ===")
for k in data:
    if k not in ['labels', 'values']:
        print(f"{k}: {data[k]}")

print()
print(f"Data points: {len(data.get('labels', []))} entries")
print(f"Date range: {data.get('labels', [None])[0]} to {data.get('labels', [None])[-1]}")
print(f"Latest value: {data.get('values', [None])[-1]} €/m²")

print()
print("=== Parameters used ===")
print("- idCategoria=1 (Residenziale)")
print("- idContratto=1 (Vendita)")
print("- idRegione=lom (Lombardia)")
print("- idProvincia=MI (Milano)")
print("- idComune=8075 (Rodano ISTAT code)")
