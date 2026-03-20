import json

snap15 = json.load(open("listings_history/snapshot_20260315_121730.json"))
snap20 = json.load(open("listings_history/snapshot_20260320_121106.json"))

print(f"March 15: {len(snap15['listings'])} listings")
print(f"March 20: {len(snap20['listings'])} listings")

has_coords_15 = sum(1 for l in snap15['listings'] if l.get('latitude') and l.get('longitude'))
has_coords_20 = sum(1 for l in snap20['listings'] if l.get('latitude') and l.get('longitude'))
print(f"March 15 with coords: {has_coords_15}")
print(f"March 20 with coords: {has_coords_20}")

ids_15 = set(l['id'] for l in snap15['listings'])
ids_20 = set(l['id'] for l in snap20['listings'])

removed = ids_15 - ids_20
new = ids_20 - ids_15

print(f"\nRemoved (15->20): {len(removed)}")
if removed:
    print(f"  Examples: {sorted(removed)[:5]}")
    
print(f"New (15->20): {len(new)}")

if removed:
    rid = list(removed)[0]
    rl = next((l for l in snap15['listings'] if l['id'] == rid), None)
    print(f"\nRemoved listing {rid}: lat={rl.get('latitude')}, lng={rl.get('longitude')}, price={rl.get('price')}")
