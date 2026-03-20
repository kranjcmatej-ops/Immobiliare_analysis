#!/usr/bin/env python3
"""Find Peschiera Borromeo commune ID on immobiliare.it and list nearby communes."""
import requests, re, json
from concurrent.futures import ThreadPoolExecutor, as_completed

H_HTML = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36', 'Accept': 'text/html'}
H_JSON = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36', 'Accept': 'application/json'}

# ── Step 1: parse idComune from the Peschiera listing & market pages ──────────
print("=" * 60)
print("Step 1: Scanning HTML pages for idComune")
print("=" * 60)
for url in [
    "https://www.immobiliare.it/vendita-case/peschiera-borromeo/",
    "https://www.immobiliare.it/prezzi-case/peschiera-borromeo/",
]:
    r = requests.get(url, headers=H_HTML, timeout=15, allow_redirects=True)
    print(f"  [{r.status_code}] {r.url}")
    if r.ok:
        hits = re.findall(r'["\']idComune["\']\s*[:=]\s*["\']?(\d+)', r.text)
        ids_near = re.findall(r'"id"\s*:\s*(\d+)', r.text[:5000])
        blobs = re.findall(r'\{[^{}]{0,400}[Pp]eschiera[^{}]{0,400}\}', r.text)
        blob_ids = []
        for b in blobs[:3]:
            blob_ids += re.findall(r'"id"\s*:\s*(\d+)', b)
        print(f"    idComune refs: {list(set(hits))}")
        print(f"    IDs in 'Peschiera' blobs: {list(set(blob_ids))[:10]}")
        # Print raw blob for context
        if blobs:
            print(f"    First blob snippet: {blobs[0][:200]}")

# ── Step 2: try direct slug-based price chart redirect ────────────────────────
print()
print("=" * 60)
print("Step 2: Slug-based prezzi-case URLs")
print("=" * 60)
for slug in ['peschiera-borromeo', 'rodano', 'segrate', 'san-donato-milanese', 'cernusco-sul-naviglio']:
    url = f"https://www.immobiliare.it/prezzi-case/{slug}/"
    r = requests.get(url, headers=H_HTML, timeout=10, allow_redirects=True)
    hits = re.findall(r'idComune[=:"\s]+(\d+)', r.text)
    hits += re.findall(r'idComune=(\d+)', r.url)
    print(f"  {slug}: status={r.status_code}  idComune={list(set(hits))[:5]}")

# ── Step 3: focused scan 8040-8140 ────────────────────────────────────────────
print()
print("=" * 60)
print("Step 3: Scanning idComune 8040-8140 (focused, 20 threads)")
print("=" * 60)

BASE = "https://www.immobiliare.it/api-next/charts/price-chart/?idCategoria=1&idContratto=1&idRegione=lom&idProvincia=MI&idComune={cid}&__lang=it"

def fetch(cid):
    try:
        r = requests.get(BASE.format(cid=cid), headers=H_JSON, timeout=8)
        if r.ok:
            d = r.json()
            name = d.get('subtitle', '')
            if name:
                return (cid, name)
    except Exception:
        pass
    return None

found = {}
with ThreadPoolExecutor(max_workers=20) as ex:
    futs = {ex.submit(fetch, cid): cid for cid in range(8040, 8141)}
    for f in as_completed(futs):
        result = f.result()
        if result:
            cid, name = result
            found[cid] = name

print(f"Found {len(found)} in range 8040-8140:")
for k in sorted(found.keys()):
    flag = "  <<<< FOUND!" if 'peschiera' in found[k].lower() else ""
    print(f"  {k}: {found[k]}{flag}")

if not any('peschiera' in v.lower() for v in found.values()):
    print()
    print("Not in 8040-8140 - trying 7800-8040 and 8140-8400...")
    found2 = {}
    with ThreadPoolExecutor(max_workers=25) as ex:
        futs = {ex.submit(fetch, cid): cid for cid in list(range(7800, 8040)) + list(range(8141, 8400))}
        for f in as_completed(futs):
            result = f.result()
            if result:
                cid, name = result
                found2[cid] = name
                if 'peschiera' in name.lower():
                    print(f"  *** FOUND: {cid}: {name} ***")
    print(f"  Found {len(found2)} more communes.")
    found.update(found2)

print()
print("=" * 60)
print("All MI communes found (sorted by name):")
print("=" * 60)
for k in sorted(found.keys(), key=lambda x: found[x]):
    print(f"  {k:6d}: {found[k]}")

headers_json = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
}

# ── Step 1: extract commune ID from the listings page ──────────────────────
print("Step 1: Extracting idComune for Peschiera Borromeo from listings URL...")
listings_url = "https://www.immobiliare.it/vendita-case/peschiera-borromeo/?__lang=it"
r = requests.get(listings_url, headers=headers_html, timeout=15, allow_redirects=True)
print(f"  Status: {r.status_code}, Final URL: {r.url}")
# Look for idComune in JSON-LD or embedded state
matches = re.findall(r'idComune["\\\\]*[=:\s]+["\\\\]*(\d+)', r.text)
print(f"  idComune matches in HTML: {list(set(matches))[:20]}")
# Also search for commune=\d+ patterns
matches2 = re.findall(r'comune["\\=:\s]+["\\]*(\d+)', r.text, re.IGNORECASE)
print(f"  commune= matches: {list(set(matches2))[:20]}")
# Look for 'peschiera' near numbers
matches3 = re.findall(r'peschiera[^"]{0,80}', r.text, re.IGNORECASE)
for m in matches3[:5]:
    print(f"  peschiera context: {m!r}")
print()

# ── Step 2: also try the market prices page directly ───────────────────────
print("Step 2: Checking market prices page URL...")
for slug in [
    'https://www.immobiliare.it/mercato-immobiliare/lombardia/milano-citta-metropolitana/peschiera-borromeo/',
    'https://www.immobiliare.it/mercato-immobiliare/lombardia/milano/peschiera-borromeo/',
]:
    r2 = requests.get(slug, headers=headers_html, timeout=15, allow_redirects=True)
    print(f"  [{r2.status_code}] {r2.url}")
    if r2.ok:
        m = re.findall(r'idComune["\\\\]*[=:\s]+["\\\\]*(\d+)', r2.text)
        print(f"  idComune hits: {list(set(m))[:10]}")
print()

# ── Step 3: scan a focused range around 8084 (alphabetically nearby) ───────
BASE = "https://www.immobiliare.it/api-next/charts/price-chart/?idCategoria=1&idContratto=1&idRegione=lom&idProvincia=MI&idComune={cid}&__lang=it"

def fetch(cid):
    try:
        r = requests.get(BASE.format(cid=cid), headers=headers_json, timeout=8)
        if r.ok:
            d = r.json()
            name = d.get('subtitle', '')
            if name:
                return (cid, name)
    except Exception:
        pass
    return None

found = {}
SCAN_RANGE = list(range(7900, 8300))
print(f"Step 3: Scanning idComune {SCAN_RANGE[0]}-{SCAN_RANGE[-1]} with 25 threads...")

with ThreadPoolExecutor(max_workers=25) as ex:
    futures = {ex.submit(fetch, cid): cid for cid in SCAN_RANGE}
    for f in as_completed(futures):
        result = f.result()
        if result:
            cid, name = result
            found[cid] = name
            if 'peschiera' in name.lower() or 'rodano' in name.lower() or 'segrate' in name.lower():
                print(f"  *** {cid}: {name} ***")

print(f"\nAll communes in range {SCAN_RANGE[0]}-{SCAN_RANGE[-1]}:")
for k in sorted(found.keys()):
    flags = []
    if 'peschiera' in found[k].lower(): flags.append("<< PESCHIERA")
    if 'rodano'    in found[k].lower(): flags.append("<< RODANO")
    if 'segrate'   in found[k].lower(): flags.append("<< SEGRATE")
    if 'cologno'   in found[k].lower(): flags.append("<< COLOGNO")
    print(f"  {k}: {found[k]}  {'  '.join(flags)}")
