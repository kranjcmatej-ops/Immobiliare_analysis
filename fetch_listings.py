"""
fetch_listings.py  Rodano property market monitor
===================================================
Saves timestamped JSON snapshots to listings_history/ on every run.
Diffs against the previous snapshot: new, removed, re-priced listings.

AUTOMATED SIZE + DETAIL FETCHING
---------------------------------
Set DATADOME_COOKIE below (paste once from DevTools > Application > Cookies).
The script will then auto-fetch full details for every listing that is missing
them: surface, rooms, bathrooms, condition, energy class, floor.
Once fetched, all data is carried forward from snapshot to snapshot.
When the cookie expires (403s appear), paste a fresh one from your browser.

To get the cookie: open any immobiliare.it page in Chrome,
DevTools > Application > Cookies > www.immobiliare.it > copy value of 'datadome'

HOW TO UPDATE THE LISTING CAPTURE
-----------------------------------
With a fresh DATADOME_COOKIE set, the script automatically discovers ALL listings
across every page. CAPTURED_LISTINGS is only needed as a fallback or to manually
override size_m2 for specific listings (e.g. the Option 2 property).

To get a fresh cookie: open any immobiliare.it page in Chrome,
DevTools > Application > Cookies > www.immobiliare.it > copy value of 'datadome'
"""

import re as _re
import os
import requests
import json
import datetime
import time
import random
import subprocess
from pathlib import Path


# ===========================================================================
# CONFIGURATION --- update these as needed
# ===========================================================================

# Paste your datadome cookie value here (from browser DevTools),
# OR set the DATADOME_COOKIE environment variable (used in CI / GitHub Actions).
# Leave empty string to skip auto-fetching (sizes will show '?' until filled manually).
DATADOME_COOKIE = os.environ.get("DATADOME_COOKIE", "")

# Full browser cookie string from DevTools 'Copy as cURL' -- paste the entire Cookie: header.
# Used for search/list pages which DataDome checks more aggressively than detail pages.
# Update this together with DATADOME_COOKIE every time you refresh the session.
# OR set the BROWSER_COOKIES environment variable (used in CI / GitHub Actions).
BROWSER_COOKIES = os.environ.get("BROWSER_COOKIES", "")

# Set True to re-fetch details for ALL listings (use when cookie expired and
# all detail data is stale, or on first run after adding many new listings).
FORCE_REFETCH = False

# Set to False to print only CHANGES SINCE LAST SNAPSHOT
# Set to True to print full diagnostic output
VERBOSE_OUTPUT = False

CAPTURE_DATE = "2026-03-14"

# price / size_m2 are the only fields you need to provide here.
# Everything else (rooms, condition, energy class, floor, etc.) is auto-fetched.
# size_m2 can also be left None and will be auto-fetched.
CAPTURED_LISTINGS = [
    {"id": "127161945", "type": "Villetta a schiera", "price": 460000, "size_m2": None},
    {"id": "125434549", "type": "Villa",              "price": 772000, "size_m2": None},
    {"id": "124775013", "type": "Appartamento",       "price": 239000, "size_m2": None},
    {"id": "121435942", "type": "Villa",              "price": 990000, "size_m2": None},
    {"id": "125907071", "type": "Appartamento",       "price": 119000, "size_m2": None},
    {"id": "126612405", "type": "Appartamento",       "price": None,   "size_m2": None},
    {"id": "126571185", "type": "Appartamento",       "price": 460000, "size_m2": None},
    {"id": "127405311", "type": "Appartamento",       "price": 189000, "size_m2": None},
    {"id": "126230351", "type": "Appartamento",       "price": 660000, "size_m2": None},
    {"id": "127479878", "type": "Appartamento",       "price": 247000, "size_m2": None},
    {"id": "125330125", "type": "Villetta a schiera", "price": 269000, "size_m2": None},
    {"id": "127059837", "type": "Appartamento",       "price": 460000, "size_m2": None},
    {"id": "125916717", "type": "Appartamento",       "price": 115000, "size_m2": None},
    {"id": "125159037", "type": "Villa",              "price": 370000, "size_m2": None},
    {"id": "119099603", "type": "Appartamento",       "price": 435000, "size_m2": None},
    {"id": "127016559", "type": "Appartamento",       "price": 139000, "size_m2": None},
    {"id": "127187915", "type": "Appartamento",       "price": 115000, "size_m2": None},
    {"id": "120768496", "type": "Villa",              "price": 359000, "size_m2": None},
    {"id": "126926993", "type": "Appartamento",       "price": 380000, "size_m2": 123.7},  # <- Option 2
    {"id": "126786619", "type": "Villetta a schiera", "price": 310000, "size_m2": None},
    {"id": "126926703", "type": "Appartamento",       "price": 320000, "size_m2": None},
    {"id": "107038817", "type": "Appartamento",       "price": 145000, "size_m2": None},
    {"id": "117873285", "type": "Appartamento",       "price": 115000, "size_m2": None},
    {"id": "126142599", "type": "Appartamento",       "price": 110000, "size_m2": None},
    {"id": "110184959", "type": "Appartamento",       "price": 180000, "size_m2": None},
]
# ===========================================================================


HEADERS_JSON = {
    "accept": "application/json, */*",
    "accept-language": "en-GB,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.3.1 Safari/605.1.15",
    "referer": "https://www.immobiliare.it/vendita-case/rodano/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

HEADERS_HTML = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-GB,en;q=0.9",
    "user-agent": HEADERS_JSON["user-agent"],
    "referer": "https://www.immobiliare.it/vendita-case/rodano/",
    "cache-control": "max-age=0",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
}

LISTINGS_DIR = Path("listings_history")

# -- Auto-discovery: fetch all listings from search page __NEXT_DATA__ -------

def auto_discover_listings():
    """
    Auto-discover all active listings by fetching every search results page and
    parsing __NEXT_DATA__. Handles pagination automatically.
    Returns list of {id, type, price, size_m2} dicts.
    Requires DATADOME_COOKIE to be set.
    """
    if not DATADOME_COOKIE and not BROWSER_COOKIES:
        if VERBOSE_OUTPUT:
            print("  [auto-discover] No cookies set -- cannot auto-discover listings.")
        return []

    all_listings = []
    page = 1
    while True:
        url = "https://www.immobiliare.it/vendita-case/rodano/"
        if page > 1:
            url += f"?pag={page}"
        cookie_str = BROWSER_COOKIES if BROWSER_COOKIES else f"datadome={DATADOME_COOKIE}"
        headers = {**HEADERS_HTML, "cookie": cookie_str}
        try:
            r = requests.get(url, headers=headers, timeout=20)
        except Exception as e:
            if VERBOSE_OUTPUT:
                print(f"  [auto-discover] Request error page {page}: {e}")
            break

        if r.status_code != 200:
            if VERBOSE_OUTPUT:
                print(f"  [auto-discover] HTTP {r.status_code} on page {page}")
            break

        nd = _re.search(r'id="__NEXT_DATA__"[^>]*>(.+?)</script>', r.text, _re.DOTALL)
        if not nd:
            if VERBOSE_OUTPUT:
                print(f"  [auto-discover] __NEXT_DATA__ not found on page {page}")
            break

        data = json.loads(nd.group(1))
        pp = data.get("props", {}).get("pageProps", {})

        # Try legacy pageProps keys first, then React Query dehydratedState
        results = None
        total_pages = 1
        for key in ("searchListData", "listData", "searchData", "listingData"):
            node = pp.get(key)
            if isinstance(node, dict):
                r_list = (node.get("results")
                          or node.get("realEstates")
                          or node.get("list"))
                if r_list:
                    results = r_list
                    total_pages = int(node.get("pages") or node.get("maxPages") or 1)
                    break

        # React Query dehydratedState (newer Next.js builds)
        if results is None:
            for q in pp.get("dehydratedState", {}).get("queries", []):
                qdata = q.get("state", {}).get("data") or {}
                if not isinstance(qdata, dict):
                    continue
                r_list = (qdata.get("results")
                          or qdata.get("realEstates")
                          or qdata.get("list"))
                if r_list:
                    results = r_list
                    total_pages = int(qdata.get("maxPages") or qdata.get("pages") or 1)
                    break

        if results is None:
            if VERBOSE_OUTPUT:
                print(f"  [auto-discover] Could not find results in __NEXT_DATA__ page {page}.")
                print(f"  [auto-discover] Available pageProps keys: {list(pp.keys())}")
            break

        page_listings = []
        for item in results:
            re_obj = item.get("realEstate", item) if isinstance(item, dict) else {}
            lid = str(re_obj.get("id") or re_obj.get("idImmobile") or "")
            if not lid:
                continue
            typ_obj = re_obj.get("typology") or re_obj.get("category") or {}
            typ = typ_obj.get("name", "Appartamento") if isinstance(typ_obj, dict) else str(typ_obj)
            price_obj = re_obj.get("price") or {}
            price = price_obj.get("value") if isinstance(price_obj, dict) else None
            # Preserve manual size override from CAPTURED_LISTINGS if present
            captured = {c["id"]: c for c in CAPTURED_LISTINGS}
            manual_size = captured.get(lid, {}).get("size_m2")
            page_listings.append({"id": lid, "type": typ, "price": price, "size_m2": manual_size})

        all_listings.extend(page_listings)
        if VERBOSE_OUTPUT:
            print(f"  [auto-discover] Page {page}/{total_pages} -- {len(page_listings)} listings")

        if page >= total_pages or not page_listings:
            break
        page += 1
        time.sleep(random.uniform(15, 35))

    if VERBOSE_OUTPUT:
        print(f"  [auto-discover] Total: {len(all_listings)} listings discovered.")
    return all_listings


# -- Listing detail auto-fetch (via __NEXT_DATA__ in listing HTML) -----------

def fetch_listing_detail(listing_id):
    """
    Fetch full listing data from immobiliare.it HTML page using the datadome cookie.
    Returns a dict with: surface_m2, price, rooms, bathrooms, condition,
    energy_class, floor, title. Returns None on failure.
    """
    if not DATADOME_COOKIE:
        return None
    url = f"https://www.immobiliare.it/annunci/{listing_id}/"
    headers = {**HEADERS_HTML, "cookie": f"datadome={DATADOME_COOKIE}"}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        nd = _re.search(r'id="__NEXT_DATA__"[^>]*>(.+?)</script>', r.text, _re.DOTALL)
        if not nd:
            return None
        d = json.loads(nd.group(1))
        re_obj = d["props"]["pageProps"]["detailData"]["realEstate"]
        p0     = re_obj["properties"][0]

        # -- Surface --
        raw_surface = p0.get("surface") or p0.get("surfaceValue", "")
        m = _re.search(r"[\d.,]+", str(raw_surface))
        surface = float(m.group().replace(",", ".")) if m else None

        # -- Price (verified from HTML, may differ from GA capture) --
        price_obj = re_obj.get("price") or p0.get("price") or {}
        price = price_obj.get("value") if isinstance(price_obj, dict) else None

        # -- Floor: take the short floorOnlyValue if available --
        floor_obj = p0.get("floor") or {}
        floor_str = floor_obj.get("floorOnlyValue") or floor_obj.get("value", "")
        # Trim to first 40 chars for display
        floor_str = floor_str[:40].strip()

        # -- Energy class --
        energy_class = None
        energy = p0.get("energy") or {}
        cls = energy.get("class")
        if isinstance(cls, dict):
            energy_class = cls.get("name")
        elif isinstance(cls, str):
            energy_class = cls

        # -- Posted/Published date --
        # createdAt is the listing creation timestamp (Unix epoch in seconds)
        posted_date = None
        if re_obj.get("createdAt"):
            try:
                ts = re_obj["createdAt"]
                # Convert Unix timestamp to ISO date string
                dt = datetime.datetime.fromtimestamp(ts)
                posted_date = dt.date().isoformat()
            except:
                posted_date = None

        return {
            "surface_m2":    surface,
            "price_verified": price,
            "rooms":         p0.get("rooms"),
            "bathrooms":     p0.get("bathrooms"),
            "condition":     p0.get("condition"),
            "energy_class":  energy_class,
            "floor":         floor_str,
            "title":         re_obj.get("title", "")[:120],
            "posted_date":   posted_date,
        }
    except Exception as e:
        return None


# -- Snapshot I/O -------------------------------------------------------------

def list_snapshots():
    return sorted(LISTINGS_DIR.glob("snapshot_*.json"))


def load_snapshot(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_latest_snapshot():
    files = list_snapshots()
    return load_snapshot(files[-1]) if files else None


def save_snapshot(data, ts):
    path = LISTINGS_DIR / f"snapshot_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


# -- Live API fetchers ---------------------------------------------------------

def fetch_price_chart():
    url = (
        "https://www.immobiliare.it/api-next/charts/price-chart/"
        "?idCategoria=1&idContratto=1&idRegione=lom&idProvincia=MI&idComune=8075&__lang=it"
    )
    try:
        r = requests.get(url, headers=HEADERS_JSON, timeout=15)
        if r.ok:
            d      = r.json()
            labels = d.get("labels", [])
            values = [float(v) for v in d.get("values", [])]
            return {
                "latest_label": labels[-1] if labels else None,
                "latest_value": values[-1] if values else None,
                "history": list(zip(labels, values)),
            }
    except Exception as e:
        if VERBOSE_OUTPUT:
            print(f"  [price-chart error] {e}")
    return None


def fetch_markers_count():
    """
    Fetch total listings count by extracting from the search page's __NEXT_DATA__.
    The /api/property-map/markers/ endpoint is broken (returns 500), so we parse
    the data that's already available on the listing page itself.
    More reliable than relying on an external API.
    """
    if not DATADOME_COOKIE and not BROWSER_COOKIES:
        return None
    
    url = "https://www.immobiliare.it/vendita-case/rodano/"
    cookie_str = BROWSER_COOKIES if BROWSER_COOKIES else f"datadome={DATADOME_COOKIE}"
    headers = {**HEADERS_HTML, "cookie": cookie_str}
    
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code != 200:
            return None
        
        nd = _re.search(r'id="__NEXT_DATA__"[^>]*>(.+?)</script>', r.text, _re.DOTALL)
        if not nd:
            return None
        
        d = json.loads(nd.group(1))
        pp = d.get("props", {}).get("pageProps", {})
        
        # Extract count from dehydratedState queries
        for q in pp.get("dehydratedState", {}).get("queries", []):
            qdata = q.get("state", {}).get("data") or {}
            if isinstance(qdata, dict) and "count" in qdata:
                return qdata["count"]
        
        return None
        
    except Exception as e:
        return None


def fetch_removed_listing_coordinates(listing_id):
    """
    Attempt to fetch coordinates for a removed listing by accessing its detail page.
    Returns {latitude, longitude, address, city} or empty dict if failed.
    This works even for delisted listings as long as the page is still accessible.
    """
    if not DATADOME_COOKIE:
        return {}
    
    url = f"https://www.immobiliare.it/annunci/{listing_id}/"
    headers = {**HEADERS_HTML, "cookie": f"datadome={DATADOME_COOKIE}"}
    
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return {}
        
        nd = _re.search(r'id="__NEXT_DATA__"[^>]*>(.+?)</script>', r.text, _re.DOTALL)
        if not nd:
            return {}
        
        d = json.loads(nd.group(1))
        re_obj = d.get("props", {}).get("pageProps", {}).get("detailData", {}).get("realEstate", {})
        
        if "properties" in re_obj and len(re_obj["properties"]) > 0:
            loc = re_obj["properties"][0].get("location") or {}
            if loc.get("latitude") and loc.get("longitude"):
                return {
                    "latitude": loc.get("latitude"),
                    "longitude": loc.get("longitude"),
                    "address": loc.get("address"),
                    "city": loc.get("city"),
                    "macrozone": loc.get("macrozone"),
                }
    except Exception as e:
        pass
    
    return {}


def fetch_listings_with_coordinates():
    """
    Fetch listings from the map API endpoint which includes full coordinate data.
    Returns a dict mapping listing_id -> {latitude, longitude, address, ...}
    This endpoint uses a geographic bounding box to fetch listings.
    """
    if not DATADOME_COOKIE and not BROWSER_COOKIES:
        return {}
    
    # Bounding box for Rodano area (from the user's network capture)
    min_lat, max_lat = 45.464345, 45.487456
    min_lng, max_lng = 9.280701, 9.404297
    
    url = "https://www.immobiliare.it/api-next/search-list/listings/"
    params = {
        "fkRegione": "lom",
        "idProvincia": "MI",
        "idComune": "8075",
        "idNazione": "IT",
        "idContratto": "1",
        "idCategoria": "1",
        "__lang": "it",
        "minLat": min_lat,
        "maxLat": max_lat,
        "minLng": min_lng,
        "maxLng": max_lng,
        "pag": 1,
        "paramsCount": 4,
        "path": "/vendita-case/rodano/"
    }
    
    headers = HEADERS_JSON.copy()
    
    coordinates_by_id = {}
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        if r.status_code != 200:
            if VERBOSE_OUTPUT:
                print(f"  [map-api] HTTP {r.status_code}")
            return {}
        
        data = r.json()
        results = data.get("results", [])
        
        for item in results:
            re_obj = item.get("realEstate", {})
            listing_id = str(re_obj.get("id") or "")
            if not listing_id:
                continue
            
            # Extract location from properties[0]
            if "properties" in re_obj and len(re_obj["properties"]) > 0:
                loc = re_obj["properties"][0].get("location") or {}
                if loc.get("latitude") and loc.get("longitude"):
                    coordinates_by_id[listing_id] = {
                        "latitude": loc.get("latitude"),
                        "longitude": loc.get("longitude"),
                        "address": loc.get("address"),
                        "city": loc.get("city"),
                        "macrozone": loc.get("macrozone"),
                        "province": loc.get("province"),
                        "region": loc.get("region"),
                    }
        
        if VERBOSE_OUTPUT:
            print(f"  [map-api] OK -- {len(coordinates_by_id)} listings with coordinates")
        return coordinates_by_id
        
    except Exception as e:
        if VERBOSE_OUTPUT:
            print(f"  [map-api] Error: {e}")
        return {}


def collect_all_removed_listings(current_ids):
    """
    Scan all snapshots to find listings no longer in the active set.
    Returns list of listing dicts enriched with a 'last_seen' date field.
    """
    seen = {}  # id -> {"data": listing_dict, "last_seen": "YYYY-MM-DD"}
    for snap_path in list_snapshots():
        snap = load_snapshot(snap_path)
        snap_time = snap.get("snapshot_time", "")
        snap_date = snap_time[:10] if snap_time else "?"
        for listing in snap.get("listings", []):
            lid = listing["id"]
            seen[lid] = {"data": listing, "last_seen": snap_date}
    result = []
    for lid, entry in seen.items():
        if lid not in current_ids:
            row = dict(entry["data"])
            row["last_seen"] = entry["last_seen"]
            result.append(row)
    return result


def collect_price_history():
    """
    Scan all snapshots to build price history for all listings.
    Returns dict: listing_id -> list of (date, price) tuples, from oldest to newest.
    """
    price_history = {}  # id -> [(date, price), ...]
    snapshots = sorted(list_snapshots())  # oldest first
    for snap_path in snapshots:
        snap = load_snapshot(snap_path)
        snap_time = snap.get("snapshot_time", "")
        snap_date = snap_time[:10] if snap_time else "?"
        for listing in snap.get("listings", []):
            lid = listing["id"]
            price = listing.get("price")
            if price is not None:
                if lid not in price_history:
                    price_history[lid] = []
                # Only add if price changed or it's a different date
                if not price_history[lid] or price_history[lid][-1][1] != price or price_history[lid][-1][0] != snap_date:
                    price_history[lid].append((snap_date, price))
    return price_history


def generate_map_html(listings, removed_listings=None, all_historical_removed=None, price_history=None, output_path="map_listings.html"):
    """
    Generate an interactive HTML map showing all listings with coordinates.
    Uses Leaflet.js for mapping and OpenStreetMap tiles.
    Can optionally show removed listings with a toggle.
    Below the map, a sortable/filterable table shows ALL listings (active + historical sold).
    Tracks and displays price changes from price_history if provided.
    """
    if removed_listings is None:
        removed_listings = []
    if all_historical_removed is None:
        all_historical_removed = []
    if price_history is None:
        price_history = {}

    # Filter listings with coordinates
    with_coords = [l for l in listings if l.get("latitude") and l.get("longitude")]
    removed_coords = [l for l in removed_listings if l.get("latitude") and l.get("longitude")]
    
    if not with_coords and not removed_coords:
        return None
    
    # Calculate center and bounds (prefer active listings)
    if with_coords:
        lats = [l["latitude"] for l in with_coords]
        lngs = [l["longitude"] for l in with_coords]
    else:
        lats = [l["latitude"] for l in removed_coords]
        lngs = [l["longitude"] for l in removed_coords]
    
    center_lat = (min(lats) + max(lats)) / 2
    center_lng = (min(lngs) + max(lngs)) / 2
    
    # Build marker data
    markers = []
    for l in with_coords:
        price_str = f"€{l['price']:,.0f}" if l.get("price") else "n/a"
        size_str = f"{l['size_m2']:.0f} m²" if l.get("size_m2") else "n/a"
        epm_str = f"€{l['eur_per_m2']:,.0f}/m²" if l.get("eur_per_m2") else "n/a"
        posted_str = l.get('posted_date', '?') if l.get('posted_date') else '?'
        
        popup_html = f"""
            <div style="font-family: Arial, sans-serif; font-size: 12px; width: 200px; background: white; padding: 8px; border-radius: 4px;">
                <b>{l['type']}</b><br>
                {l['address']}, {l['city']}<br>
                <b>Price:</b> {price_str}<br>
                <b>Size:</b> {size_str}<br>
                <b>EUR/m²:</b> {epm_str}<br>
                <b>Rooms:</b> {l.get('rooms', '?')}<br>
                <b>Posted:</b> {posted_str}<br>
                <a href="{l['url']}" target="_blank" style="color: #3498db;">View on immobiliare.it</a>
            </div>
        """
        
        # Color code by price range
        if l.get("price"):
            if l["price"] < 200000:
                color = "green"
            elif l["price"] < 400000:
                color = "blue"
            elif l["price"] < 600000:
                color = "orange"
            else:
                color = "red"
        else:
            color = "gray"
        
        markers.append({
            "lat": l["latitude"],
            "lng": l["longitude"],
            "popup": popup_html,
            "color": color,
            "price": l.get("price", 0),
            "id": l["id"],
            "removed": False
        })
    
    # Add removed listings as separate markers
    for l in removed_coords:
        price_str = f"€{l['price']:,.0f}" if l.get("price") else "n/a"
        size_str = f"{l['size_m2']:.0f} m²" if l.get("size_m2") else "n/a"
        epm_str = f"€{l['eur_per_m2']:,.0f}/m²" if l.get("eur_per_m2") else "n/a"
        posted_str = l.get('posted_date', '?') if l.get('posted_date') else '?'
        
        days_ago = (datetime.date.today() - datetime.date.fromisoformat(l.get("first_seen", "2020-01-01"))).days
        
        popup_html = f"""
            <div style="font-family: Arial, sans-serif; font-size: 12px; width: 200px; background: #fff3cd; padding: 8px; border-radius: 4px; border: 1px solid #ffc107;">
                <b style="color: #856404;">❌ REMOVED/SOLD</b><br>
                <b>{l['type']}</b><br>
                {l['address']}, {l['city']}<br>
                <b>Last price:</b> {price_str}<br>
                <b>Size:</b> {size_str}<br>
                <b>EUR/m²:</b> {epm_str}<br>
                <b>Rooms:</b> {l.get('rooms', '?')}<br>
                <b>Posted:</b> {posted_str}<br>
                <i>On market for {days_ago} days</i><br>
                <a href="{l['url']}" target="_blank" style="color: #856404;">View archive</a>
            </div>
        """
        
        markers.append({
            "lat": l["latitude"],
            "lng": l["longitude"],
            "popup": popup_html,
            "color": "gray",
            "price": l.get("price", 0),
            "id": l["id"],
            "removed": True,
            "days_ago": days_ago
        })
    
    # Build combined table data: all active + all historically removed
    table_rows = []
    
    # Helper to calculate price change info
    def get_price_change_info(lid, current_price):
        if lid not in price_history or len(price_history[lid]) < 2:
            return None, None, None
        history = price_history[lid]
        # Find the most recent price change by comparing consecutive entries
        # history is sorted oldest -> newest
        for i in range(len(history) - 1, 0, -1):
            if history[i][1] != history[i-1][1]:
                # Found a price change
                prev_price = history[i-1][1]
                curr_price_hist = history[i][1]
                if prev_price is None or curr_price_hist is None:
                    return None, None, None
                delta = curr_price_hist - prev_price
                delta_pct = round((delta / prev_price) * 100, 1) if prev_price else 0
                # Format the price change string
                direction = "↑" if delta > 0 else "↓"
                price_change_str = f"{direction} €{abs(delta):,.0f} ({delta_pct:+.1f}%)"
                return delta, delta_pct, price_change_str
        # No price change found in history
        return None, None, None
    
    for l in listings:
        delta, delta_pct, change_str = get_price_change_info(l["id"], l.get("price"))
        table_rows.append({
            "id": l["id"],
            "type": l.get("type", ""),
            "title": l.get("title", ""),
            "price": l.get("price"),
            "eur_per_m2": l.get("eur_per_m2"),
            "size_m2": l.get("size_m2"),
            "rooms": l.get("rooms", ""),
            "energy_class": l.get("energy_class", ""),
            "posted_date": l.get("posted_date", ""),
            "url": l.get("url", ""),
            "status": "active",
            "last_seen": None,
            "price_change_delta": delta,
            "price_change_pct": delta_pct,
            "price_change_str": change_str,
        })

    # Use all_historical_removed for the table (richer history), fall back to removed_listings
    historical_for_table = all_historical_removed if all_historical_removed else removed_listings
    active_ids = {l["id"] for l in listings}
    for l in historical_for_table:
        delta, delta_pct, change_str = get_price_change_info(l["id"], l.get("price"))
        table_rows.append({
            "id": l["id"],
            "type": l.get("type", ""),
            "title": l.get("title", ""),
            "price": l.get("price"),
            "eur_per_m2": l.get("eur_per_m2"),
            "size_m2": l.get("size_m2"),
            "rooms": l.get("rooms", ""),
            "energy_class": l.get("energy_class", ""),
            "posted_date": l.get("posted_date", ""),
            "url": l.get("url", ""),
            "status": "sold",
            "last_seen": l.get("last_seen", "?"),
            "price_change_delta": delta,
            "price_change_pct": delta_pct,
            "price_change_str": change_str,
        })

    # Sort by posted_date descending (None/empty at bottom)
    def sort_key(r):
        d = r.get("posted_date") or ""
        return d if d else "0000"
    table_rows.sort(key=sort_key, reverse=True)

    table_rows_json = json.dumps(table_rows)

    # HTML template
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Rodano Real Estate Listings Map</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css" />
        <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/1.5.1/MarkerCluster.min.css" />
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/1.5.1/MarkerCluster.Default.min.css" />
        <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/1.5.1/leaflet.markercluster.min.js"></script>
        <style>
            * {{ box-sizing: border-box; }}
            html, body {{ height: 100%; margin: 0; padding: 0; font-family: Arial, sans-serif; }}
            #map {{ height: 62vh; width: 100%; }}
            .info {{
                padding: 10px 12px;
                font: 14px/16px Arial, Helvetica, sans-serif;
                background: white;
                background: rgba(255,255,255,0.95);
                box-shadow: 0 0 15px rgba(0,0,0,0.2);
                border-radius: 5px;
                max-width: 250px;
            }}
            .info h4 {{ margin: 0 0 8px 0; font-size: 15px; }}
            .legend {{
                line-height: 20px;
                color: #555;
                font-size: 13px;
            }}
            .legend i {{
                width: 14px;
                height: 14px;
                float: left;
                margin-right: 8px;
                opacity: 0.7;
                border-radius: 50%;
                border: 2px solid white;
            }}
            .checkbox-item {{
                margin-top: 10px;
                padding-top: 8px;
                border-top: 1px solid #ddd;
            }}
            .checkbox-item input {{
                margin-right: 6px;
            }}
            .checkbox-item label {{
                cursor: pointer;
                font-size: 13px;
            }}
            .stats {{
                font-size: 12px;
                margin-top: 8px;
                padding-top: 8px;
                border-top: 1px solid #ddd;
                color: #666;
            }}
            /* Table section */
            #table-section {{
                padding: 16px 20px 40px;
                background: #f8f9fa;
            }}
            #table-section h2 {{
                margin: 0 0 12px 0;
                font-size: 18px;
                color: #333;
            }}
            #table-controls {{
                display: flex;
                align-items: center;
                gap: 18px;
                margin-bottom: 10px;
                flex-wrap: wrap;
            }}
            #search-box {{
                padding: 6px 10px;
                border: 1px solid #ccc;
                border-radius: 4px;
                font-size: 14px;
                width: 240px;
            }}
            #row-count {{
                font-size: 13px;
                color: #666;
            }}
            .toggle-label {{
                font-size: 13px;
                cursor: pointer;
                user-select: none;
            }}
            .toggle-label input {{
                margin-right: 5px;
            }}
            #listings-table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 13px;
                background: white;
                box-shadow: 0 1px 4px rgba(0,0,0,0.08);
                border-radius: 6px;
                overflow: hidden;
            }}
            #listings-table thead th {{
                background: #2c3e50;
                color: white;
                padding: 10px 8px;
                text-align: left;
                cursor: pointer;
                white-space: nowrap;
                user-select: none;
            }}
            #listings-table thead th:hover {{
                background: #34495e;
            }}
            #listings-table thead th .sort-arrow {{
                margin-left: 4px;
                opacity: 0.5;
                font-size: 11px;
            }}
            #listings-table thead th.sorted-asc .sort-arrow::after {{ content: "▲"; opacity: 1; }}
            #listings-table thead th.sorted-desc .sort-arrow::after {{ content: "▼"; opacity: 1; }}
            #listings-table thead th:not(.sorted-asc):not(.sorted-desc) .sort-arrow::after {{ content: "⇅"; }}
            #listings-table tbody tr {{
                border-bottom: 1px solid #eee;
                transition: background 0.15s;
            }}
            #listings-table tbody tr.row-active:hover {{ background: #e8f4fd; }}
            #listings-table tbody tr.row-sold {{ background: #fffbe6; }}
            #listings-table tbody tr.row-sold:hover {{ background: #fff3cd; }}
            #listings-table td {{
                padding: 8px 8px;
                vertical-align: middle;
            }}
            #listings-table td.sold-text {{ color: #888; }}
            .badge-active {{
                display: inline-block;
                padding: 2px 8px;
                border-radius: 10px;
                background: #27ae60;
                color: white;
                font-size: 11px;
                white-space: nowrap;
            }}
            .badge-sold {{
                display: inline-block;
                padding: 2px 8px;
                border-radius: 10px;
                background: #e74c3c;
                color: white;
                font-size: 11px;
                white-space: nowrap;
            }}
            .price-change {{
                display: inline-block;
                padding: 3px 8px;
                border-radius: 12px;
                font-size: 11px;
                white-space: nowrap;
                font-weight: 500;
            }}
            .price-increase {{
                background: #d5f4e6;
                color: #27ae60;
            }}
            .price-decrease {{
                background: #fadbd8;
                color: #c0392b;
            }}
            a.listing-link {{ color: #2980b9; text-decoration: none; }}
            a.listing-link:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <div id="map"></div>

        <div id="table-section">
            <h2>All Listings — Rodano</h2>
            <div id="table-controls">
                <input type="text" id="search-box" placeholder="Filter by title, type, energy…" oninput="applyFilters()">
                <label class="toggle-label"><input type="checkbox" id="show-active" checked onchange="applyFilters()"> Show Active</label>
                <label class="toggle-label"><input type="checkbox" id="show-sold" checked onchange="applyFilters()"> Show Sold</label>
                <span id="row-count"></span>
            </div>
            <table id="listings-table">
                <thead>
                    <tr>
                        <th onclick="sortTable(0)" data-col="0">Posted <span class="sort-arrow"></span></th>
                        <th onclick="sortTable(1)" data-col="1">Type <span class="sort-arrow"></span></th>
                        <th onclick="sortTable(2)" data-col="2">Title <span class="sort-arrow"></span></th>
                        <th onclick="sortTable(3)" data-col="3">Price <span class="sort-arrow"></span></th>
                        <th onclick="sortTable(9)" data-col="9">Change <span class="sort-arrow"></span></th>
                        <th onclick="sortTable(4)" data-col="4">€/m² <span class="sort-arrow"></span></th>
                        <th onclick="sortTable(5)" data-col="5">m² <span class="sort-arrow"></span></th>
                        <th onclick="sortTable(6)" data-col="6">Rooms <span class="sort-arrow"></span></th>
                        <th onclick="sortTable(7)" data-col="7">Energy <span class="sort-arrow"></span></th>
                        <th onclick="sortTable(8)" data-col="8">Status <span class="sort-arrow"></span></th>
                    </tr>
                </thead>
                <tbody id="table-body"></tbody>
            </table>
        </div>

        <script>
            // ── Map setup ───────────────────────────────────────────────────
            const map = L.map('map').setView([{center_lat}, {center_lng}], 13);
            
            L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
                attribution: '© OpenStreetMap contributors',
                maxZoom: 19
            }}).addTo(map);
            
            const markers = {json.dumps(markers)};
            
            const colorMap = {{
                'green': '#2ecc71',
                'blue': '#3498db',
                'orange': '#f39c12',
                'red': '#e74c3c',
                'gray': '#95a5a6'
            }};
            
            const activeMarkers = L.markerClusterGroup({{ maxClusterRadius: 40 }});
            const removedMarkers = L.markerClusterGroup({{ maxClusterRadius: 40 }});
            
            markers.forEach(marker => {{
                const isRemoved = marker.removed === true;
                const circleMarker = L.circleMarker([marker.lat, marker.lng], {{
                    color: 'white',
                    fillColor: isRemoved ? '#bdc3c7' : colorMap[marker.color],
                    fillOpacity: isRemoved ? 0.4 : 0.7,
                    weight: isRemoved ? 1 : 2,
                    opacity: isRemoved ? 0.6 : 1,
                    radius: 7,
                    dashArray: isRemoved ? '2, 2' : undefined
                }}).bindPopup(marker.popup);
                (isRemoved ? removedMarkers : activeMarkers).addLayer(circleMarker);
            }});
            
            activeMarkers.addTo(map);
            
            const info = L.control({{position: 'topleft'}});
            info.onAdd = function(map) {{
                const div = L.DomUtil.create('div', 'info');
                div.innerHTML = '<h4>Rodano Listings</h4>' +
                    '<div class="legend">' +
                    '<i style="background: #2ecc71;"></i> < €200k<br>' +
                    '<i style="background: #3498db;"></i> €200k – €400k<br>' +
                    '<i style="background: #f39c12;"></i> €400k – €600k<br>' +
                    '<i style="background: #e74c3c;"></i> > €600k<br>' +
                    '</div>';
                L.DomEvent.disableClickPropagation(div);
                map.removeLayer(removedMarkers);
                return div;
            }};
            info.addTo(map);

            // ── Table setup ─────────────────────────────────────────────────
            const ALL_ROWS = {table_rows_json};

            let sortCol = 0;
            let sortAsc = false; // default: posted_date descending

            function fmt(v, prefix, suffix) {{
                if (v == null || v === '') return '<span style="color:#bbb">–</span>';
                return (prefix||'') + Number(v).toLocaleString('it-IT') + (suffix||'');
            }}

            function renderTable(rows) {{
                const tbody = document.getElementById('table-body');
                if (!rows.length) {{
                    tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:20px;color:#888">No listings match</td></tr>';
                    document.getElementById('row-count').textContent = '0 listings';
                    return;
                }}
                const html = rows.map(r => {{
                    const sold = r.status === 'sold';
                    const rowClass = sold ? 'row-sold' : 'row-active';
                    const textClass = sold ? ' sold-text' : '';
                    const badge = sold
                        ? `<span class="badge-sold">Sold ~${{r.last_seen || '?'}}</span>`
                        : `<span class="badge-active">Active</span>`;
                    const titleCell = r.url
                        ? `<a class="listing-link" href="${{r.url}}" target="_blank">${{r.title || r.id}}</a>`
                        : (r.title || r.id);
                    const priceChangeHtml = (r.price_change_str != null) ?
                        `<span class="price-change ${{r.price_change_delta > 0 ? 'price-increase' : 'price-decrease'}}">${{r.price_change_str}}</span>`
                        : '<span style="color:#bbb">–</span>';
                    return `<tr class="${{rowClass}}">
                        <td class="${{textClass}}">${{r.posted_date || '–'}}</td>
                        <td class="${{textClass}}">${{r.type || '–'}}</td>
                        <td>${{titleCell}}</td>
                        <td class="${{textClass}}">${{fmt(r.price, '€')}}</td>
                        <td class="${{textClass}}">${{priceChangeHtml}}</td>
                        <td class="${{textClass}}">${{fmt(r.eur_per_m2, '€', '/m²')}}</td>
                        <td class="${{textClass}}">${{fmt(r.size_m2, '', ' m²')}}</td>
                        <td class="${{textClass}}">${{r.rooms || '–'}}</td>
                        <td class="${{textClass}}">${{r.energy_class || '–'}}</td>
                        <td>${{badge}}</td>
                    </tr>`;
                }}).join('');
                tbody.innerHTML = html;
                document.getElementById('row-count').textContent = rows.length + ' listing' + (rows.length !== 1 ? 's' : '');
            }}

            function applyFilters() {{
                const query = document.getElementById('search-box').value.toLowerCase();
                const showActive = document.getElementById('show-active').checked;
                const showSold = document.getElementById('show-sold').checked;
                let rows = ALL_ROWS.filter(r => {{
                    if (r.status === 'active' && !showActive) return false;
                    if (r.status === 'sold' && !showSold) return false;
                    if (query) {{
                        const haystack = [r.title, r.type, r.energy_class, r.rooms, r.posted_date]
                            .filter(Boolean).join(' ').toLowerCase();
                        if (!haystack.includes(query)) return false;
                    }}
                    return true;
                }});
                rows = sortRows(rows, sortCol, sortAsc);
                renderTable(rows);
            }}

            function sortRows(rows, col, asc) {{
                const keys = ['posted_date','type','title','price','price_change_delta','eur_per_m2','size_m2','rooms','energy_class','status'];
                const key = keys[col];
                return [...rows].sort((a, b) => {{
                    let av = a[key], bv = b[key];
                    if (av == null) av = asc ? '\uffff' : '';
                    if (bv == null) bv = asc ? '\uffff' : '';
                    if (typeof av === 'number' && typeof bv === 'number') return asc ? av - bv : bv - av;
                    return asc ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
                }});
            }}

            function sortTable(col) {{
                if (sortCol === col) {{ sortAsc = !sortAsc; }}
                else {{ sortCol = col; sortAsc = col !== 0 && col !== 3 && col !== 4 && col !== 5; }}
                // Update header arrows
                document.querySelectorAll('#listings-table thead th').forEach((th, i) => {{
                    th.classList.remove('sorted-asc', 'sorted-desc');
                    if (i === sortCol) th.classList.add(sortAsc ? 'sorted-asc' : 'sorted-desc');
                }});
                applyFilters();
            }}

            // Initial render — mark col 0 as sorted desc
            document.querySelectorAll('#listings-table thead th')[0].classList.add('sorted-desc');
            applyFilters();
        </script>
    </body>
    </html>
    """
    
    try:
        path = Path(output_path)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        return path
    except Exception as e:
        print(f"  [map-html] Error: {e}")
        return None


# -- Diff logic ----------------------------------------------------------------

def diff_snapshots(prev, curr):
    prev_by_id = {l["id"]: l for l in prev.get("listings", [])}
    curr_by_id = {l["id"]: l for l in curr.get("listings", [])}
    prev_ids   = set(prev_by_id)
    curr_ids   = set(curr_by_id)

    repriced = []
    for lid in prev_ids & curr_ids:
        p, c = prev_by_id[lid].get("price"), curr_by_id[lid].get("price")
        if p is not None and c is not None and p != c:
            repriced.append({
                "id":        lid,
                "type":      curr_by_id[lid]["type"],
                "old_price": p,
                "new_price": c,
                "delta":     c - p,
                "delta_pct": round((c - p) / p * 100, 1),
            })

    return {
        "new":       [curr_by_id[i] for i in sorted(curr_ids - prev_ids)],
        "removed":   [prev_by_id[i] for i in sorted(prev_ids - curr_ids)],
        "repriced":  repriced,
        "unchanged": len(prev_ids & curr_ids) - len(repriced),
    }


# -- Helpers ------------------------------------------------------------------

def days_on_market(first_seen, today):
    try:
        return (today - datetime.date.fromisoformat(first_seen)).days
    except Exception:
        return 0


def fmt_price(price):
    return f"EUR{price:,}" if price is not None else "n/a"


# -- GitHub push helper -------------------------------------------------------

def push_map_to_github():
    """
    Stage, commit, and push map_listings.html to GitHub.
    Runs only if git is available and we're in a git repository.
    """
    try:
        # Check if we're in a git repo and have changes
        result = subprocess.run(
            ["git", "status", "--porcelain", "map_listings.html"],
            capture_output=True,
            text=True,
            timeout=10,
            cwd="."
        )
        if result.returncode != 0 or not result.stdout.strip():
            return

        # Stage the file
        subprocess.run(
            ["git", "add", "map_listings.html"],
            capture_output=True,
            timeout=10,
            cwd="."
        )

        # Commit with timestamp
        now = datetime.datetime.now()
        commit_msg = f"Update map_listings.html - {now.strftime('%Y-%m-%d %H:%M:%S')}"
        result = subprocess.run(
            ["git", "commit", "-m", commit_msg],
            capture_output=True,
            text=True,
            timeout=10,
            cwd="."
        )

        if result.returncode != 0:
            print(f"  [git] Commit failed: {result.stderr.strip()}")
            return

        # Push to origin
        result = subprocess.run(
            ["git", "push"],
            capture_output=True,
            text=True,
            timeout=30,
            cwd="."
        )

        if result.returncode == 0:
            print(f"  [git] Pushed to GitHub ✓")
        else:
            print(f"  [git] Push failed: {result.stderr.strip()}")

    except subprocess.TimeoutExpired:
        print("  [git] Operation timed out")
    except FileNotFoundError:
        print("  [git] Git not found - skipping push")
    except Exception as e:
        print(f"  [git] Error: {e}")


# -- Main ---------------------------------------------------------------------

def main():
    LISTINGS_DIR.mkdir(exist_ok=True)
    now   = datetime.datetime.now()
    today = now.date()

    print("=" * 62)
    print(f"  Rodano listing monitor  --  {now.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 62)

    # Previous snapshot
    prev = load_latest_snapshot()
    if prev:
        print(f"\n  Previous snapshot : {prev.get('snapshot_time', '?')}  ({len(prev.get('listings', []))} listings)")
    else:
        print("\n  No previous snapshot -- this will be the first.")

    # Resolve listing source: always auto-discover (all pages) when cookie is set;
    # fall back to CAPTURED_LISTINGS when discovery fails or no cookie available.
    if DATADOME_COOKIE:
        print("\nAuto-discovering listings (fetching all search pages) ...")
        active_listings = auto_discover_listings()
        if not active_listings:
            if CAPTURED_LISTINGS:
                print(f"  Auto-discovery returned nothing. Falling back to {len(CAPTURED_LISTINGS)} listings from CAPTURED_LISTINGS.")
                active_listings = CAPTURED_LISTINGS
            else:
                print("  Auto-discovery returned nothing and CAPTURED_LISTINGS is empty.")
                active_listings = []
    else:
        active_listings = CAPTURED_LISTINGS
        if active_listings:
            print(f"\n  Using {len(active_listings)} listings from CAPTURED_LISTINGS (no cookie for auto-discovery).")
        else:
            print("\n  No cookie and CAPTURED_LISTINGS is empty. Set DATADOME_COOKIE to enable auto-discovery.")

    # Live API data
    print("\nFetching live data ...")
    chart         = fetch_price_chart()
    markers_count = fetch_markers_count()
    coordinates   = fetch_listings_with_coordinates()

    if chart:
        print(f"  Price chart  OK  --  {chart['latest_label']}  EUR{chart['latest_value']:,.0f}/m2")
    else:
        print("  Price chart  FAILED")

    if markers_count is not None:
        live_count = len(active_listings)
        note = ""
        if markers_count != live_count:
            note = f"  <<  markers={markers_count} vs discovered={live_count}  -- consider re-running"
        print(f"  Markers API  OK  --  {markers_count} listings on map{note}")
    else:
        print("  Markers API  FAILED")

    # Enrich listings: carry fields from previous snapshot;
    # auto-fetch full detail for new listings or those still missing detail.
    prev_by_id = {l["id"]: l for l in prev.get("listings", [])} if prev else {}
    need_fetch = []
    for raw in active_listings:
        prev_e = prev_by_id.get(raw["id"], {})
        is_new = raw["id"] not in prev_by_id
        has_detail = bool(prev_e.get("detail_fetched")) and not FORCE_REFETCH
        if DATADOME_COOKIE and (is_new or not has_detail):
            need_fetch.append(raw["id"])

    if need_fetch:
        print(f"\nFetching detail for {len(need_fetch)} listing(s) ...")
    fetched_details: dict = {}
    for i, lid in enumerate(need_fetch):
        if i > 0:
            delay = random.uniform(10, 25)
            time.sleep(delay)
        detail = fetch_listing_detail(lid)
        if detail:
            fetched_details[lid] = detail
            surface = detail.get("surface_m2")
            print(f"  [{lid}]  {surface} m\u00b2  {detail.get('condition','')}  {detail.get('rooms','')} rooms  energy {detail.get('energy_class','')}")
        else:
            print(f"  [{lid}]  fetch failed (cookie expired?)")

    print("\nBuilding snapshot ...")
    enriched = []
    for raw in active_listings:
        prev_e  = prev_by_id.get(raw["id"], {})
        manual_size = raw.get("size_m2")  # explicit override always wins
        detail  = fetched_details.get(raw["id"]) or {}
        loc_data = coordinates.get(raw["id"]) or {}

        # Size: manual override > freshly fetched > previous snapshot
        size = manual_size or detail.get("surface_m2") or prev_e.get("size_m2")

        # Price: GA capture > verified-from-HTML (GA is more current for monitoring)
        price = raw.get("price") if raw.get("price") is not None else (
            detail.get("price_verified") or prev_e.get("price")
        )

        entry = {
            "id":            raw["id"],
            "type":          raw["type"],
            "price":         price,
            "size_m2":        size,
            "eur_per_m2":    round(price / size) if (price and size) else None,
            "url":           f"https://www.immobiliare.it/annunci/{raw['id']}/",
            "first_seen":    (
                prev_e.get("first_seen", today.isoformat())
                if raw["id"] in prev_by_id else today.isoformat()
            ),
            # detail fields: prefer fresh fetch, fall back to stored
            "rooms":         detail.get("rooms")         or prev_e.get("rooms"),
            "bathrooms":     detail.get("bathrooms")     or prev_e.get("bathrooms"),
            "condition":     detail.get("condition")     or prev_e.get("condition"),
            "energy_class":  detail.get("energy_class")  or prev_e.get("energy_class"),
            "floor":         detail.get("floor")         or prev_e.get("floor"),
            "title":         detail.get("title")         or prev_e.get("title"),
            "posted_date":   detail.get("posted_date")   or prev_e.get("posted_date"),
            "detail_fetched": (
                today.isoformat() if detail else prev_e.get("detail_fetched")
            ),
            # location/coordinate fields: prefer fresh fetch, fall back to stored
            "latitude":      loc_data.get("latitude")    or prev_e.get("latitude"),
            "longitude":     loc_data.get("longitude")   or prev_e.get("longitude"),
            "address":       loc_data.get("address")     or prev_e.get("address"),
            "location":      loc_data.get("macrozone")   or prev_e.get("location"),
            "city":          loc_data.get("city")        or prev_e.get("city"),
        }
        enriched.append(entry)

    # Build snapshot
    snapshot = {
        "snapshot_time": now.isoformat(timespec="seconds"),
        "capture_date":  CAPTURE_DATE,
        "listings":      enriched,
        "market_index":  chart,
        "markers_count": markers_count,
    }

    # Diff report
    if prev:
        diff = diff_snapshots(prev, snapshot)
        n_new      = len(diff["new"])
        n_removed  = len(diff["removed"])
        n_repriced = len(diff["repriced"])

        print("\n" + "-" * 62)
        print("  CHANGES SINCE LAST SNAPSHOT")
        print("-" * 62)

        if n_new == 0 and n_removed == 0 and n_repriced == 0:
            print("  No changes detected.")
        else:
            if diff["new"]:
                print(f"\n  NEW LISTINGS ({n_new})")
                for l in diff["new"]:
                    print(f"    + [{l['id']}]  {l['type']:<22}  {fmt_price(l['price']):>12}  {l['url']}")

            if diff["removed"]:
                print(f"\n  REMOVED / LIKELY SOLD ({n_removed})")
                for l in diff["removed"]:
                    dom = days_on_market(l.get("first_seen", today.isoformat()), today)
                    url = f"https://www.immobiliare.it/annunci/{l['id']}/"
                    print(f"    - [{l['id']}]  {l['type']:<22}  {fmt_price(l['price']):>12}  {dom}d on market  {url}")

            if diff["repriced"]:
                print(f"\n  PRICE CHANGES ({n_repriced})")
                for r in diff["repriced"]:
                    arrow = "^" if r["delta"] > 0 else "v"
                    print(f"    ~ [{r['id']}]  {r['type']:<22}  {fmt_price(r['old_price'])} -> {fmt_price(r['new_price'])}  {arrow}{abs(r['delta_pct'])}%")

        if diff["unchanged"]:
            print(f"\n  Unchanged: {diff['unchanged']} listings")

    # Current listings table
    if VERBOSE_OUTPUT:
        print("\n" + "-" * 90)
        print("  CURRENT LISTINGS")
        print("-" * 90)
        print(f"  {'#':<3} {'ID':<12} {'Type':<22} {'Price':>12}  {'m2':>5}  {'EUR/m2':>7}  {'Rooms':>5}  {'Cond':<20}  {'Enrg'}  {'DOM':>4}d")
        print(f"  {'-'*3} {'-'*12} {'-'*22} {'-'*12}  {'-'*5}  {'-'*7}  {'-'*5}  {'-'*20}  {'-'*4}  {'-'*5}")
        for i, l in enumerate(enriched, 1):
            dom       = days_on_market(l["first_seen"], today)
            long_note = " LONG" if dom >= 60 else ""
            opt2_note = "  <- Option 2" if l["id"] == "126926993" else ""
            size_str  = f"{l['size_m2']:.0f}" if l.get("size_m2") else "?"
            epm_str   = f"{l['eur_per_m2']:,}" if l.get("eur_per_m2") else "?"
            rooms_str = str(l.get("rooms") or "?")
            cond_str  = (l.get("condition") or "?")[:20]
            enrg_str  = l.get("energy_class") or "?"
            print(f"  {i:<3} {l['id']:<12} {l['type']:<22} {fmt_price(l['price']):>12}  {size_str:>5}  {epm_str:>7}  {rooms_str:>5}  {cond_str:<20}  {enrg_str:<4}  {dom:>4}{long_note}{opt2_note}")

    # Appartamenti summary
    if VERBOSE_OUTPUT:
        apts   = [l for l in enriched if l["type"] == "Appartamento" and l.get("price")]
        prices = sorted(l["price"] for l in apts)
        if apts:
            print(f"\n  Appartamenti with price: {len(apts)}")
            print(f"  Min    : {fmt_price(min(prices))}")
            print(f"  Median : {fmt_price(prices[len(prices) // 2])}")
            print(f"  Mean   : {fmt_price(sum(prices) // len(prices))}")
            print(f"  Max    : {fmt_price(max(prices))}")
            apts_with_size = [l for l in apts if l.get("eur_per_m2")]
            if apts_with_size:
                epm_vals = sorted(l["eur_per_m2"] for l in apts_with_size)
                print(f"  Appartamenti with size+price: {len(apts_with_size)}")
                print(f"  EUR/m2 min    : {min(epm_vals):,}")
                print(f"  EUR/m2 median : {epm_vals[len(epm_vals) // 2]:,}")
                print(f"  EUR/m2 mean   : {sum(epm_vals) // len(epm_vals):,}")
                print(f"  EUR/m2 max    : {max(epm_vals):,}")

    # Market index (last 13 months)
    if VERBOSE_OUTPUT and chart:
        print("\n" + "-" * 62)
        print("  MARKET INDEX  (immobiliare.it asking EUR/m2, Rodano)")
        print("-" * 62)
        for lbl, val in chart["history"][-13:]:
            bar = "#" * int(val / 100)
            print(f"  {lbl:<10}  {val:>7,.0f}  {bar}")

    # Snapshot history
    path = save_snapshot(snapshot, now)
    all_snaps = list_snapshots()
    print(f"  Saved  ->  {path.name}")
    if VERBOSE_OUTPUT:
        print(f"  All snapshots in {LISTINGS_DIR}/  ({len(all_snaps)} total):")
        for s in all_snaps:
            snap = load_snapshot(s)
            cnt  = len(snap.get("listings", []))
            print(f"    {s.name}   ({cnt} listings)")
    
    # Generate map HTML
    removed_listings = diff["removed"] if prev else []
    
    # Enrich removed listings with coordinates
    if removed_listings:
        print(f"  Enriching {len(removed_listings)} removed listing(s) with coordinates...")
        need_coord_fetch = []
        for r_listing in removed_listings:
            # First check previous snapshot
            prev_entry = prev_by_id.get(r_listing["id"], {})
            r_listing["latitude"] = r_listing.get("latitude") or prev_entry.get("latitude")
            r_listing["longitude"] = r_listing.get("longitude") or prev_entry.get("longitude")
            r_listing["address"] = r_listing.get("address") or prev_entry.get("address")
            r_listing["city"] = r_listing.get("city") or prev_entry.get("city")
            r_listing["location"] = r_listing.get("location") or prev_entry.get("location")
            
            # If still no coordinates, mark for fetching
            if not (r_listing.get("latitude") and r_listing.get("longitude")):
                need_coord_fetch.append(r_listing["id"])
        
        # Fetch coordinates for removed listings that still don't have them
        if need_coord_fetch and DATADOME_COOKIE:
            for rid in need_coord_fetch:
                loc_data = fetch_removed_listing_coordinates(rid)
                if loc_data:
                    r_listing = next((r for r in removed_listings if r["id"] == rid), None)
                    if r_listing:
                        r_listing["latitude"] = loc_data.get("latitude")
                        r_listing["longitude"] = loc_data.get("longitude")
                        r_listing["address"] = loc_data.get("address")
                        r_listing["city"] = loc_data.get("city")
                        r_listing["location"] = loc_data.get("macrozone")
                        if VERBOSE_OUTPUT:
                            print(f"    [OK] {rid} coordinates fetched")
                time.sleep(3)  # Gentle delay between detail fetches
            
    map_path = generate_map_html(enriched, removed_listings=removed_listings, all_historical_removed=collect_all_removed_listings({l["id"] for l in enriched}), price_history=collect_price_history())
    if map_path:
        print(f"  Map saved  ->  {map_path.name}")
        if removed_listings:
            with_coords = sum(1 for r in removed_listings if r.get("latitude") and r.get("longitude"))
            print(f"  Showing {with_coords}/{len(removed_listings)} removed listings on map")
        
        # Push map to GitHub (skip in CI -- the workflow handles git operations)
        if not os.environ.get("CI"):
            push_map_to_github()
    else:
        print("  Map generation  SKIPPED (no listings with coordinates)")
    
    print("=" * 62)


if __name__ == "__main__":
    main()
