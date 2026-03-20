import json

# ── Data ──
complex_txns = [
    {"date": "2022-03", "price": 292500, "apt_m2": 121, "box_m2": 23},
    {"date": "2022-03", "price": 170000, "apt_m2": 78,  "box_m2": 16},
    {"date": "2022-04", "price": 201340, "apt_m2": 99,  "box_m2": 22},
    {"date": "2022-06", "price": 225375, "apt_m2": 90,  "box_m2": 30},
    {"date": "2022-12", "price": 115000, "apt_m2": 77,  "box_m2": 23},
    {"date": "2022-12", "price": 170000, "apt_m2": 71,  "box_m2": 22},
    {"date": "2023-01", "price": 290000, "apt_m2": 103, "box_m2": 22},
    {"date": "2023-01", "price": 310000, "apt_m2": 195, "box_m2": 22},
    {"date": "2023-03", "price": 234966, "apt_m2": 86,  "box_m2": 22},
    {"date": "2023-04", "price": 280000, "apt_m2": 89,  "box_m2": 0},
    {"date": "2023-04", "price": 247000, "apt_m2": 73,  "box_m2": 17},
    {"date": "2024-07", "price": 370000, "apt_m2": 146, "box_m2": 22},
    {"date": "2024-10", "price": 255000, "apt_m2": 94,  "box_m2": 17},
    {"date": "2025-04", "price": 305000, "apt_m2": 103, "box_m2": 22},
]

GARAGE_VALUE = 16000

with open("listings_history/snapshot_20260314_190728.json") as f:
    snapshot = json.load(f)

market_idx = {d: v for d, v in snapshot["market_index"]["history"]}
latest_idx = 2293.0

def get_index(date_str):
    key = date_str + "-01"
    if key in market_idx:
        return market_idx[key]
    y, m = date_str.split("-")
    for offset in [0, -1, 1, -2, 2]:
        mo = int(m) + offset
        yr = int(y)
        if mo < 1: mo += 12; yr -= 1
        if mo > 12: mo -= 12; yr += 1
        k = f"{yr}-{mo:02d}-01"
        if k in market_idx:
            return market_idx[k]
    return None

# Prepare time-adjusted data
for t in complex_txns:
    box_val = GARAGE_VALUE if t["box_m2"] > 0 else 0
    t["apt_value"] = t["price"] - box_val
    t["eur_m2_adj"] = t["apt_value"] / t["apt_m2"]
    idx = get_index(t["date"])
    t["time_adj_eur_m2"] = t["eur_m2_adj"] * (latest_idx / idx) if idx else None

# Target
TARGET_COMM = 123.7
TARGET_CALP = 100
TARGET_BOX = 16

# ═══════════════════════════════════════════════════════════════
# METHOD 1: REGRESSION ON COMPLEX TRANSACTIONS
# ═══════════════════════════════════════════════════════════════
print("=" * 80)
print("METHOD 1a: REGRESSION ON COMPLEX TRANSACTIONS (time-adjusted)")
print("=" * 80)

# Exclude outliers: Dec 2022 77m² (€115k, likely intra-family) and Jan 2023 195m² (extreme size)
filtered = [t for t in complex_txns
            if t.get("time_adj_eur_m2") and t["apt_m2"] < 180 and t["eur_m2_adj"] > 1400]
filtered.sort(key=lambda x: x["apt_m2"])

print(f"\n{'Date':<12} {'Apt m²':>7} {'Time-adj €/m²':>14}")
print("-" * 35)
for t in filtered:
    print(f"{t['date']:<12} {t['apt_m2']:>7} {t['time_adj_eur_m2']:>14,.0f}")

n = len(filtered)
sum_x = sum(t["apt_m2"] for t in filtered)
sum_y = sum(t["time_adj_eur_m2"] for t in filtered)
sum_xy = sum(t["apt_m2"] * t["time_adj_eur_m2"] for t in filtered)
sum_x2 = sum(t["apt_m2"]**2 for t in filtered)

b = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x**2)
a = (sum_y - b * sum_x) / n

pred_eur_m2 = a + b * TARGET_COMM
apt_value_reg = pred_eur_m2 * TARGET_COMM
total_reg = apt_value_reg + GARAGE_VALUE

print(f"\nRegression: €/m² = {a:.0f} + ({b:.2f}) × m²")
print(f"Predicted €/m² for {TARGET_COMM} m²: €{pred_eur_m2:,.0f}")
print(f"Apartment value: {TARGET_COMM} × {pred_eur_m2:,.0f} = €{apt_value_reg:,.0f}")
print(f"+ Box 16 m²: €{GARAGE_VALUE:,}")
print(f"= TOTAL: €{total_reg:,.0f}")

# ═══════════════════════════════════════════════════════════════
# METHOD 1b: WEIGHTED COMPARABLES
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("METHOD 1b: WEIGHTED COMPARABLE APPROACH")
print("=" * 80)

comps = [
    {"label": "Apr 2025, 103 m²", "t": complex_txns[13]},
    {"label": "Jul 2024, 146 m²", "t": complex_txns[11]},
    {"label": "Oct 2024, 94 m²",  "t": complex_txns[12]},
    {"label": "Jan 2023, 103 m²", "t": complex_txns[6]},
    {"label": "Mar 2022, 121 m²", "t": complex_txns[0]},
]

import math
weights = []
for c in comps:
    t = c["t"]
    size_dist = abs(t["apt_m2"] - TARGET_COMM)
    year = int(t["date"][:4])
    recency = year - 2021
    w = recency / max(size_dist, 5)
    weights.append(w)

total_w = sum(weights)
weights = [w/total_w for w in weights]

weighted_eur_m2 = sum(c["t"]["time_adj_eur_m2"] * w for c, w in zip(comps, weights))
apt_value_wt = weighted_eur_m2 * TARGET_COMM
total_wt = apt_value_wt + GARAGE_VALUE

print("\nComparables and weights:")
for c, w in zip(comps, weights):
    t = c["t"]
    print(f"  {c['label']:25s}  €{t['time_adj_eur_m2']:,.0f}/m²  weight: {w:.1%}")
print(f"\nWeighted €/m²: €{weighted_eur_m2:,.0f}")
print(f"Apartment: {TARGET_COMM} × {weighted_eur_m2:,.0f} = €{apt_value_wt:,.0f}")
print(f"+ Box: €{GARAGE_VALUE:,}")
print(f"= TOTAL: €{total_wt:,.0f}")

# ═══════════════════════════════════════════════════════════════
# METHOD 1c: MOST RECENT COMPARABLE, SCALED
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("METHOD 1c: MOST RECENT COMPARABLE + SCALING")
print("=" * 80)

t_recent = complex_txns[13]  # Apr 2025
size_ratio = TARGET_COMM / t_recent["apt_m2"]
size_adj_factor = 1 - (TARGET_COMM - t_recent["apt_m2"]) * abs(b) / t_recent["time_adj_eur_m2"]
time_factor = t_recent["time_adj_eur_m2"] / t_recent["eur_m2_adj"]

scaled_apt = t_recent["apt_value"] * size_ratio * size_adj_factor * time_factor
box_adj = GARAGE_VALUE * (TARGET_BOX / t_recent["box_m2"])
total_scaled = scaled_apt + box_adj

print(f"\nBase: Apr 2025, 103 m² + 22 m² box = €305,000")
print(f"Size ratio: {size_ratio:.3f}, Size discount factor: {size_adj_factor:.3f}")
print(f"Time factor: {time_factor:.4f}")
print(f"Scaled apartment: €{scaled_apt:,.0f}")
print(f"+ Box (16 m²): €{box_adj:,.0f}")
print(f"= TOTAL: €{total_scaled:,.0f}")

# ═══════════════════════════════════════════════════════════════
# METHOD 2: LISTING COMPARISON
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("METHOD 2: CURRENT LISTING COMPARISON (via Giusti 28)")
print("=" * 80)

giusti28_listings = [
    {"desc": "220 m², 5+r, A1, Ottimo",     "price": 660000, "m2": 220, "eur_m2": 3000},
    {"desc": "144 m², 4r, A+, Nuovo",        "price": 435000, "m2": 144, "eur_m2": 3021},
    {"desc": "123.7 m², 3r, A1 (TARGET)",    "price": 380000, "m2": 123.7, "eur_m2": 3072},
    {"desc": "110 m², 3r, A1, Ottimo",       "price": 320000, "m2": 110, "eur_m2": 2909},
]

print("\nCurrent listings at via Giusti 28:")
for l in giusti28_listings:
    marker = " <<<" if l["m2"] == TARGET_COMM else ""
    print(f"  {l['desc']:40s}  €{l['price']:>7,}  (€{l['eur_m2']:,}/m²){marker}")

other_listings = [l for l in giusti28_listings if l["m2"] != TARGET_COMM]
avg_listing_eur = sum(l["eur_m2"] for l in other_listings) / len(other_listings)
print(f"\nAvg listing €/m² (other Giusti 28 units): €{avg_listing_eur:,.0f}")

print("\nDiscounted listing estimates (typical Italian market 5-12% discount):")
listing_estimates = {}
for discount in [0.05, 0.08, 0.10, 0.12]:
    txn_eur = avg_listing_eur * (1 - discount)
    total = txn_eur * TARGET_COMM + GARAGE_VALUE * (TARGET_BOX / 22)
    listing_estimates[f"Listings -{discount:.0%}"] = total
    print(f"  {discount:.0%} discount: €{txn_eur:,.0f}/m² -> €{total:,.0f}")

# ═══════════════════════════════════════════════════════════════
# METHOD 3: BROADER A02 RODANO CONTEXT
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("METHOD 3: BROADER RODANO A02 MARKET CONTEXT")
print("=" * 80)

a02_recent = [
    {"date": "2024-07", "m2": 146, "adj_eur": 2425},
    {"date": "2024-10", "m2": 94,  "adj_eur": 2543},
    {"date": "2025-04", "m2": 103, "adj_eur": 2806},
    {"date": "2025-05", "m2": 120, "adj_eur": 2825},
    {"date": "2025-06", "m2": 82,  "adj_eur": 2427},
    {"date": "2025-07", "m2": 118, "adj_eur": 1814},
    {"date": "2025-08", "m2": 131, "adj_eur": 1771},
    {"date": "2025-11", "m2": 86,  "adj_eur": 2023},
]

vals = sorted([t["adj_eur"] for t in a02_recent])
median_a02 = (vals[len(vals)//2 - 1] + vals[len(vals)//2]) / 2
mean_a02 = sum(vals) / len(vals)

complex_recent = [t for t in complex_txns if t["date"] >= "2024" and t.get("time_adj_eur_m2")]
complex_avg = sum(t["time_adj_eur_m2"] for t in complex_recent) / len(complex_recent)
premium = complex_avg / mean_a02

print(f"\nAll Rodano A02 (2024-2025) adj €/m²: Mean €{mean_a02:,.0f} | Median €{median_a02:,.0f}")
print(f"Complex avg (2024+, time-adj): €{complex_avg:,.0f}/m²")
print(f"Complex premium vs Rodano A02 avg: {premium:.0%}")

# ═══════════════════════════════════════════════════════════════
# METHOD 4: MARKET TREND
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("METHOD 4: MARKET INDEX TREND")
print("=" * 80)

milestones = [
    ("2020-01", "Pre-COVID"),
    ("2022-01", "2022"),
    ("2023-01", "2023"),
    ("2024-01", "2024"),
    ("2025-01", "2025"),
    ("2026-02", "Latest"),
]
print("\nRodano avg listing price/m²:")
for date, label in milestones:
    key = date + "-01" if date != "2026-02" else "2026-02-01"
    val = market_idx.get(key, latest_idx)
    print(f"  {label:12s} ({date}): €{val:,.0f}/m²")

g3 = (latest_idx / market_idx["2023-03-01"] - 1) * 100
g1 = (latest_idx / market_idx["2025-02-01"] - 1) * 100
print(f"\n  3-year growth: +{g3:.1f}%")
print(f"  1-year growth: +{g1:.1f}%")

# ═══════════════════════════════════════════════════════════════
# FINAL SYNTHESIS
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("FINAL VALUATION SYNTHESIS")
print("=" * 80)

print(f"\nSubject: Via Giusti 28, Complesso Terracielo, Rodano")
print(f"  Sup. commerciale: {TARGET_COMM} m²  |  Sup. calpestabile: {TARGET_CALP} m²")
print(f"  Box: {TARGET_BOX} m²  |  Garden: Yes")

estimates = {
    "1a Regression (complex txns)":      total_reg,
    "1b Weighted Comparables":           total_wt,
    "1c Recent Scaled (Apr 2025)":       total_scaled,
    "2  Listings -8% discount":          listing_estimates["Listings -8%"],
    "2  Listings -10% discount":         listing_estimates["Listings -10%"],
}

print(f"\n{'Method':<35s} {'Estimate':>12s}")
print("-" * 50)
for method, val in estimates.items():
    print(f"  {method:<33s} €{val:>10,.0f}")

all_vals = list(estimates.values())
avg_est = sum(all_vals) / len(all_vals)
min_est = min(all_vals)
max_est = max(all_vals)

print(f"\n  Mean of methods:    €{avg_est:,.0f}")
print(f"  Range:              €{min_est:,.0f} – €{max_est:,.0f}")

# Round to nearest 5k
central = round(avg_est / 5000) * 5000
low = round(min_est / 5000) * 5000
high = round(max_est / 5000) * 5000

print(f"\n{'=' * 50}")
print(f"  ESTIMATED FAIR MARKET VALUE")
print(f"  Range:   €{low:,} – €{high:,}")
print(f"  Central: €{central:,}")
print(f"{'=' * 50}")

listing = 380000
print(f"\n  Current listing price:    €{listing:,}")
print(f"  vs Central estimate:      {(listing - central)/central:+.1%}")
print(f"  Listing appears {'OVERPRICED' if listing > high else 'FAIRLY PRICED' if listing >= low else 'UNDERPRICED'}")

# ═══════════════════════════════════════════════════════════════
# PART 2: FRAZIONAMENTO & RECOVERY COST ADJUSTMENT
# (Costs split between two units by millesimi)
# ═══════════════════════════════════════════════════════════════
print("\n\n")
print("*" * 80)
print("  PART 2: ADJUSTED VALUATION (FRAZIONAMENTO + RECOVERY COSTS)")
print("  Costs shared between two resulting units by millesimi")
print("*" * 80)

# ── MILLESIMI CALCULATION ──
UNIT_BUYER = 123.7   # m² - larger unit (buyer)
UNIT_OTHER = 87.18   # m² - smaller unit
TOTAL_ORIGINAL = UNIT_BUYER + UNIT_OTHER
MILL_BUYER = UNIT_BUYER / TOTAL_ORIGINAL
MILL_OTHER = UNIT_OTHER / TOTAL_ORIGINAL

print(f"\n  Original unit: {TOTAL_ORIGINAL:.2f} m²")
print(f"  Split into:")
print(f"    Unit A (buyer):  {UNIT_BUYER} m²  → millesimi {MILL_BUYER:.4f} ({MILL_BUYER*100:.2f}%)")
print(f"    Unit B (other):  {UNIT_OTHER} m²  → millesimi {MILL_OTHER:.4f} ({MILL_OTHER*100:.2f}%)")

# ── A. ADMINISTRATIVE / PROFESSIONAL COSTS (divided by millesimi) ──
print("\n" + "=" * 80)
print("A. ADMINISTRATIVE & PROFESSIONAL COSTS (divided by millesimi)")
print("=" * 80)

admin_costs = {
    "CILA (Comunicazione Inizio Lavori Asseverata)": (800, 1500),
    "Progetto tecnico (geometra/architetto)":        (1500, 3000),
    "Aggiornamento catastale DOCFA (x2 unita)":      (1600, 3000),
    "Diritti catastali (x2 @ ~€50)":                 (100, 100),
    "Diritti di segreteria comunali":                 (200, 500),
    "APE - Attestato Prestazione Energetica (x2)":    (300, 500),
    "Variazione tabelle millesimali condominio":      (500, 1500),
    "Pratica notarile (frazionamento atto)":          (1500, 3000),
}

print(f"\n{'Item':<52s} {'Total Lo':>9s} {'Total Hi':>9s} {'Buyer Lo':>9s} {'Buyer Hi':>9s}")
print("-" * 92)
admin_low_total = 0
admin_high_total = 0
for item, (lo, hi) in admin_costs.items():
    buyer_lo = int(lo * MILL_BUYER)
    buyer_hi = int(hi * MILL_BUYER)
    print(f"  {item:<50s} {lo:>8,} {hi:>8,} {buyer_lo:>8,} {buyer_hi:>8,}")
    admin_low_total += lo
    admin_high_total += hi
admin_low_buyer = int(admin_low_total * MILL_BUYER)
admin_high_buyer = int(admin_high_total * MILL_BUYER)
print("-" * 92)
print(f"  {'Subtotal':<50s} {admin_low_total:>8,} {admin_high_total:>8,} {admin_low_buyer:>8,} {admin_high_buyer:>8,}")
print(f"  Buyer share ({MILL_BUYER*100:.1f}% millesimi)")

# ── B. CONSTRUCTION / RENOVATION COSTS (divided by millesimi) ──
print("\n" + "=" * 80)
print("B. CONSTRUCTION & RENOVATION COSTS (divided by millesimi)")
print("=" * 80)
print(f"  (Splitting {TOTAL_ORIGINAL:.2f} m² into {UNIT_BUYER} m² + {UNIT_OTHER} m²)")
print("  Assumes ~15-20 linear meters of new walls, pipe rerouting,")
print("  new electrical panel, finishes where walls are built")

construction_costs = {
    "Pareti divisorie/tramezzi (~18 lm x 2.7h = ~49 m²)": (4000, 7500),
    "Spostamento impianto idraulico (tubi, scarichi)":     (3000, 6000),
    "Adeguamento impianto elettrico (nuovo quadro, linee)":(2000, 4000),
    "Adeguamento riscaldamento (valvole, split circuiti)": (1500, 3500),
    "Nuova porta d'ingresso blindata":                     (1200, 2500),
    "Porte interne aggiuntive (x2-3)":                     (600, 1500),
    "Intonacatura e tinteggiatura pareti nuove":           (1500, 3000),
    "Ripristino pavimentazione (dove tagliata/nuovi muri)":(1000, 2500),
    "Controsoffitti/passaggi impiantistici":                (500, 1500),
    "Smaltimento macerie e pulizia cantiere":               (500, 1000),
}

print(f"\n{'Item':<52s} {'Total Lo':>9s} {'Total Hi':>9s} {'Buyer Lo':>9s} {'Buyer Hi':>9s}")
print("-" * 92)
constr_low_total = 0
constr_high_total = 0
for item, (lo, hi) in construction_costs.items():
    buyer_lo = int(lo * MILL_BUYER)
    buyer_hi = int(hi * MILL_BUYER)
    print(f"  {item:<50s} {lo:>8,} {hi:>8,} {buyer_lo:>8,} {buyer_hi:>8,}")
    constr_low_total += lo
    constr_high_total += hi
constr_low_buyer = int(constr_low_total * MILL_BUYER)
constr_high_buyer = int(constr_high_total * MILL_BUYER)
print("-" * 92)
print(f"  {'Subtotal':<50s} {constr_low_total:>8,} {constr_high_total:>8,} {constr_low_buyer:>8,} {constr_high_buyer:>8,}")
print(f"  Buyer share ({MILL_BUYER*100:.1f}% millesimi)")

# ── C. GARDEN RECOVERY (100% buyer) ──
print("\n" + "=" * 80)
print("C. GARDEN RECOVERY (100% buyer - not part of split)")
print("=" * 80)
print("  Assumes ~60-100 m² garden, completely dead lawn")
print("  Garden cost is NOT divided — fully borne by buyer")

garden_costs = {
    "Rimozione prato morto e preparazione terreno":  (800, 1500),
    "Nuovo terreno vegetale / ammendante":           (500, 1000),
    "Semina o posa prato a rotoli (60-100 m²)":      (900, 2500),
    "Impianto irrigazione base (se assente)":         (1000, 2500),
    "Potatura/pulizia aree verdi":                    (300, 600),
}

print(f"\n{'Item':<52s} {'Low':>9s} {'High':>9s}")
print("-" * 73)
garden_low = 0
garden_high = 0
for item, (lo, hi) in garden_costs.items():
    print(f"  {item:<50s} {lo:>8,} {hi:>8,}")
    garden_low += lo
    garden_high += hi
print("-" * 73)
print(f"  {'Subtotal Garden (100% buyer)':<50s} {garden_low:>8,} {garden_high:>8,}")

# ── D. ADDITIONAL RISK / CONTINGENCY ──
print("\n" + "=" * 80)
print("D. ADDITIONAL CONSIDERATIONS")
print("=" * 80)

print("  - Frazionamento timeline: typically 3-6 months (CILA + works + catasto)")
print("  - During this period, unit is NOT habitable/sellable as standalone")
print("  - Buyer bears opportunity cost and financing cost during this period")
print("  - Risk: unexpected structural/impiantistic issues during split")

# Contingency on buyer's share of split costs + full garden
buyer_works_low = admin_low_buyer + constr_low_buyer + garden_low
buyer_works_high = admin_high_buyer + constr_high_buyer + garden_high
contingency_low = int(buyer_works_low * 0.10)
contingency_high = int(buyer_works_high * 0.15)
print(f"\n  Contingency (10-15% of buyer's costs):      {contingency_low:>7,} - {contingency_high:>7,}")

# Time value of money / opportunity cost
time_cost = int(345000 * 0.035 * 4 / 12)
print(f"  Financing/opportunity cost (~4 months):     {time_cost:>7,}")

# ── TOTAL COST SUMMARY (buyer's share only) ──
print("\n" + "=" * 80)
print("TOTAL COST SUMMARY — BUYER'S SHARE ONLY")
print("=" * 80)

buyer_total_low = admin_low_buyer + constr_low_buyer + garden_low + contingency_low + time_cost
buyer_total_high = admin_high_buyer + constr_high_buyer + garden_high + contingency_high + time_cost
buyer_total_mid = (buyer_total_low + buyer_total_high) // 2

print(f"\n{'Category':<52s} {'Buyer Lo':>9s} {'Buyer Hi':>9s}  {'Note':>20s}")
print("-" * 95)
print(f"  {'A. Admin/Professional':<50s} {admin_low_buyer:>8,} {admin_high_buyer:>8,}  {'millesimi ' + f'{MILL_BUYER*100:.1f}%':>20s}")
print(f"  {'B. Construction/Renovation':<50s} {constr_low_buyer:>8,} {constr_high_buyer:>8,}  {'millesimi ' + f'{MILL_BUYER*100:.1f}%':>20s}")
print(f"  {'C. Garden Recovery':<50s} {garden_low:>8,} {garden_high:>8,}  {'100% buyer':>20s}")
print(f"  {'D. Contingency + Time cost':<50s} {contingency_low + time_cost:>8,} {contingency_high + time_cost:>8,}")
print("=" * 95)
print(f"  {'TOTAL BUYER COSTS':<50s} {buyer_total_low:>8,} {buyer_total_high:>8,}")
print(f"  {'MIDPOINT':<50s} {buyer_total_mid:>8,}")

# For reference, show what total costs are and what other unit pays
other_admin_low = admin_low_total - admin_low_buyer
other_admin_high = admin_high_total - admin_high_buyer
other_constr_low = constr_low_total - constr_low_buyer
other_constr_high = constr_high_total - constr_high_buyer
other_total_low = other_admin_low + other_constr_low
other_total_high = other_admin_high + other_constr_high

print(f"\n  For reference:")
print(f"    Total split costs (A+B):     €{admin_low_total + constr_low_total:,} – €{admin_high_total + constr_high_total:,}")
print(f"    Other unit pays ({MILL_OTHER*100:.1f}%):       €{other_total_low:,} – €{other_total_high:,}")
print(f"    Buyer pays (A+B @{MILL_BUYER*100:.1f}%+C+D): €{buyer_total_low:,} – €{buyer_total_high:,}")

# ── ADJUSTED VALUATION ──
print("\n" + "=" * 80)
print("ADJUSTED VALUATION (Market Value - Buyer's Recovery Costs)")
print("=" * 80)

base_central = central  # from Part 1
base_low = low
base_high = high

adj_central = base_central - buyer_total_mid
adj_low = base_low - buyer_total_high
adj_high = base_high - buyer_total_low

print(f"\n  BASE market value (Part 1):   €{base_low:,} – €{base_high:,} (central €{base_central:,})")
print(f"  Buyer's recovery costs:       €{buyer_total_low:,} – €{buyer_total_high:,} (mid €{buyer_total_mid:,})")
print(f"\n{'=' * 60}")
print(f"  ADJUSTED FAIR VALUE")
print(f"  Range:   €{adj_low:,} – €{adj_high:,}")

adj_central_rounded = round(adj_central / 5000) * 5000
adj_low_rounded = round(adj_low / 5000) * 5000
adj_high_rounded = round(adj_high / 5000) * 5000

print(f"  Central: €{adj_central_rounded:,}")
print(f"{'=' * 60}")

print(f"\n  Current listing price:        €{listing:,}")
diff = listing - adj_central_rounded
pct = diff / adj_central_rounded * 100
print(f"  vs Adjusted central:          {diff:+,} ({pct:+.1f}%)")

print(f"\n  A fair negotiation target would be: €{adj_central_rounded:,} – €{adj_central_rounded + 10000:,}")
print(f"  Asking €{listing:,} is significantly above adjusted value")
