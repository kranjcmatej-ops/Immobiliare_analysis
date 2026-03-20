
# Rodano Real Estate Market Analysis (Extended Dataset)
Generated: 2026-03-14T11:05:51.816196 UTC

Source:
- Transaction records from Agenzia delle Entrate (OMI zone B1) provided by user
- Separate dataset of pertinenze (garages / storage)
- Derived calculations from the conversation

Location:
- Municipality: Rodano (MI), Italy
- OMI Zone: B1 (central consolidated residential area)

---
# Property Category Codes

| Code | Meaning |
|---|---|
| A02 | Civil residential apartment |
| A03 | Economic apartment |
| A07 | Villino / small villa |
| C06 | Garage / box |
| C02 | Storage / cantina |

---
# Target Property (Option 2)

| Metric | Value |
|---|---|
| Superficie commerciale | 123.7 m² |
| Superficie calpestabile | 100 m² |
| Garage | 16 m² |
| Asking price | €380,000 |

## Price Metrics

| Metric | Calculation | Result |
|---|---|---|
| Price per m² (commercial) | 360000 / 123.7 | ~€2,910 |
| Price per m² (net living) | 360000 / 100 | €3,600 |

Estimated garage value (Rodano market average):

| Metric | Value |
|---|---|
| Typical garage €/m² | ~€1,000 |
| Estimated garage price | ~€16,000 |

Adjusted apartment value:

| Metric | Value |
|---|---|
| Apartment value excluding garage | ~€344,000 |
| Price per m² (net apartment) | ~€3,440/m² |

---
# Estimated Market Ranges From Transactions

| Category | Typical €/m² |
|---|---|
| A03 apartments | €2,000 – €2,300 |
| A02 apartments | €2,500 – €2,900 |
| A07 villas | €1,800 – €2,400 |
| C06 garages | €900 – €1,100 |

---
# Aggregated Market Trend

| Year | Estimated Residential €/m² |
|---|---|
| 2023 | 2,400 – 2,600 |
| 2024 | 2,400 – 2,550 |
| 2025 | 2,550 – 2,750 |

Estimated overall growth (2023–2025): **~5–8%**

---
# Offer Scenario

Offer: €320,000

| Calculation | Result |
|---|---|
| Apartment price excluding garage | €304,000 |
| Price per m² | ~€3,040/m² |

If renovation + fractioning costs €10k–€15k:

| Total Cost | Effective €/m² |
|---|---|
| €330k | ~€3,140 |
| €335k | ~€3,190 |

---
# OMI Reference (2025 S1)

Abitazioni civili (Rodano B1):

| Condition | €/m² |
|---|---|
| Normale | 1550–2000 |
| Ottimo | 2050–2800 |

Box:

| Condition | €/m² |
|---|---|
| Normale | 750–1200 |

Note: OMI values typically sit **10–20% below actual transaction prices**.

---
# Raw Dataset – Residential Transactions

(Chronological excerpts exactly as provided)

```
Residenziale - Gennaio 2023
Corrispettivo: 290000
PER C06 22 m2
RES A02 103 m2

Residenziale - Gennaio 2023
Corrispettivo: 310000
PER C06 22 m2
RES A02 195 m2

Corrispettivo: 140000
PER C02 7 m2
PER C06 14 m2
RES A03 92 m2

Corrispettivo: 70000
RES A03 66 m2

Corrispettivo: 604000
PER C06 35 m2
PER C06 35 m2
RES A07 358 m2

Residenziale - Marzo 2023
Corrispettivo: 250000
PER C06 28 m2
RES A07 149 m2

Residenziale - Marzo 2023
Corrispettivo: 234966
PER C06 22 m2
RES A02 86 m2

Residenziale - Aprile 2023
Corrispettivo: 280000
RES A02 89 m2

Residenziale - Aprile 2023
Corrispettivo: 247000
PER C06 17 m2
RES A02 73 m2

Residenziale - Maggio 2023
Corrispettivo: 360000
PER C06 34 m2
RES A07 203 m2

Residenziale - Luglio 2023
Corrispettivo: 330000
PER C06 24 m2
RES A07 162 m2

Residenziale - Ottobre 2023
Corrispettivo: 600000
PER C06 34 m2
RES A07 273 m2

Residenziale - Novembre 2023
Corrispettivo: 215000
PER C06 14 m2
RES A03 100 m2

Residenziale - Dicembre 2023
Corrispettivo: 150000
RES A03 66 m2

...
(remaining records truncated for readability but preserved in original dataset)
```

---
# Raw Dataset – Garage / Pertinenze Transactions

```
Aprile 2023
€33,000 – C06 27 m2

Maggio 2023
€14,400 – C06 16 m2

Giugno 2023
€18,000 – C06 17 m2

Giugno 2023
€22,000 – C06 35 m2

Luglio 2023
€17,000 – C06 17 m2

Febbraio 2024
€17,000 – C06 14 m2

Luglio 2024
€22,500 – C06 16 m2

Marzo 2025
€19,000 – C06 23 m2

Giugno 2025
€25,000 – C06 24 m2

Luglio 2025
€15,000 – C06 18 m2

Luglio 2025
€18,500 – C06 17 m2

Settembre 2025
€30,000 – C06 37 m2

Novembre 2025
€20,000 – C06 20 m2
```

---
# Data Limitations

The dataset does not allow perfect separation of:

- garden value
- exact garage share inside bundled sales
- commercial surface for each historical sale

However the data is still sufficient to estimate market ranges.

---
# Summary

1. Rodano residential prices increased moderately from 2023 to 2025.
2. Typical apartment transaction prices cluster around €2,600–€2,800 per m².
3. Garage values average around €1,000/m².
4. The target apartment asking price implies a premium relative to historical sales.
5. An offer around €320k remains consistent with the historical data once renovation and fractioning costs are considered.

