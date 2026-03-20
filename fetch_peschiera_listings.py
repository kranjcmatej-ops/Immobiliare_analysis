"""
fetch_peschiera_listings.py  Peschiera Borromeo property market monitor
========================================================================
Saves timestamped JSON snapshots to peschiera_listings_history/ on every run.
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
1. Visit https://www.immobiliare.it/vendita-case/peschiera-borromeo/ with browser DevTools
2. Network tab, filter 'collect' in cgapigw
3. Extract IDs / prices from the GA payload (ecomm_prodid, ecomm_totalvalue)
4. Replace CAPTURED_LISTINGS below, update CAPTURE_DATE
5. Run: python fetch_peschiera_listings.py
"""

import re as _re
import requests
import json
import datetime
import time
import random
from pathlib import Path


# ===========================================================================
# CONFIGURATION --- update these as needed
# ===========================================================================

# Paste your datadome cookie value here (from browser DevTools).
# Leave empty string to skip auto-fetching (sizes will show '?' until filled manually).
DATADOME_COOKIE = "KrC5OItLhgcySbKaCK4sShFD_e8Nh2~X9g_WFsCCd0XTFaP2UqAlyz4ZZM5aVKOTtDjnGZsYThT5cRDIE8R2JIGwxy7HAqK~ZFXMJC08Tdr49ui2Tduiu~deKvz6BYY0"

# Full browser cookie string from DevTools 'Copy as cURL' -- paste the entire Cookie: header.
# Used for search/list pages which DataDome checks more aggressively than detail pages.
# Update this together with DATADOME_COOKIE every time you refresh the session.
BROWSER_COOKIES = "datadome=KrC5OItLhgcySbKaCK4sShFD_e8Nh2~X9g_WFsCCd0XTFaP2UqAlyz4ZZM5aVKOTtDjnGZsYThT5cRDIE8R2JIGwxy7HAqK~ZFXMJC08Tdr49ui2Tduiu~deKvz6BYY0; FPID=FPID2.2.sLZb5nAwSALblrY6qykEYQcLia15Gmuee1X3tkPZ9EU%3D.1750495843; _fbp=fb.1.1773244268476.1552174632; _ga_57HD1CXZCY=GS2.1.s1773569480$o23$g1$t1773572211$j59$l0$h1679250901; _ga_MKQ0HTYZFW=GS2.1.s1773569480$o21$g1$t1773572211$j60$l0$h100525501; _gtmeec=eyJlbSI6IjVjMmQ4NDIzYzEyN2IzNzcxYjdhZDIyMzJkNWVlNmU0MjU4ZmU0YzM1ZWNkZjNkZmYwZDA0ZDY5OThlYTdjNWMiLCJleHRlcm5hbF9pZCI6ImY1MjI0M2E2MmRmZmQ1M2U2YzYxN2UxYWFhNzY3ODUzNTBmNjI4OTBlZDdhY2M4YzI4OWNkYjgyYzkwNmM2OGMifQ%3D%3D; FPGSID=1.1773571301.1773572210.G-57HD1CXZCY.Ay5cbeJXGxkLTe7bDwCqMQ; __eoi=ID=7dfe9df37b4d89ce:T=1767028479:RT=1773572210:S=AA-AfjbbNG2PYgaBOLjdlGS3hhvC; __gads=ID=3ea810158ec499b9:T=1750495855:RT=1773572210:S=ALNI_MaQ_NzuP8Gmi4PYFJzq7mDpdNdMTA; __gpi=UID=00001155ba3d0597:T=1750495855:RT=1773572210:S=ALNI_MYOM3BXJvVIzHqikhLhjte5fvRBdA; __rtbh.ssgtm.aid=Xv6aN3Goex41zcHtlQq; ab.storage.deviceId.fe67ee0d-3b20-47d7-95bc-dbbe2778b467=g%3A4a28ba29-ca75-9a4a-75d1-4bf8602e2690%7Ce%3Aundefined%7Cc%3A1750495842866%7Cl%3A1773569480099; ab.storage.sessionId.fe67ee0d-3b20-47d7-95bc-dbbe2778b467=g%3A15e0a8d6-1fda-4e6d-afa4-1decf30b9c46%7Ce%3A1773574010342%7Cc%3A1773569480098%7Cl%3A1773572210342; ab.storage.userId.fe67ee0d-3b20-47d7-95bc-dbbe2778b467=g%3A6ddf3486-5ec7-505b-be44-ca4120061ae0%7Ce%3Aundefined%7Cc%3A1750495847653%7Cl%3A1773569480100; ajs_anonymous_id=c4455846-93b7-4db5-bb09-ce5877d71625; ajs_user_id=6ddf3486-5ec7-505b-be44-ca4120061ae0; _ga=GA1.1.1463115632.1750495843; _uetsid=41923016d2814eb5878f441e804829c3; _uetvid=5a7314b5ce6e44959be264bcf3d9e423; __rtbh.aid=%7B%22eventType%22%3A%22aid%22%2C%22id%22%3A%22Xv6aN3Goex41zcHtlQq%22%2C%22expiryDate%22%3A%222027-03-15T10%3A52%3A07.358Z%22%7D; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22BClLpaeaoNzMPuTFg0nx%22%2C%22expiryDate%22%3A%222027-03-15T10%3A52%3A07.358Z%22%7D; __dedupev2=other; __dedupe=other; __utmz=other; IM_PAP_TI=eyJudW0iOjMsInVybCI6ImFiMjYzNjU1YTg4ZWQyZTY3MjZiZmMwMzhlMWJlYWQwIn0=; FPLC=K2VwYYsFUHiXPd9RGnZiSHZRK725WfMi3%2FpAJ7DTY77NLCo8Esoi43EhiqLdj%2BcpJYotxBcORIb%2FansjRtw4mhI4aeyB%2FW8mL%2FKiItP4gc1yIKA3n0yKC30F7qBzRA%3D%3D; crto_is_user_optout=false; crto_mapped_user_id=HxWxhl9sTktuaGxlUG5RdCUyRlNsOVFZT1pGZlFlWEo0JTJCZklmc1pLSFMlMkZxRVBadEMySERNWEdLMUQzaGxKYjRXM2cxcnk4dHJEbWJlY1NUJTJCdFZiRE5LWHI5THZBJTNEJTNE; cto_bundle=qOrpk19mS2YlMkJFJTJCQmJnV3VIelVlZjRlMGoyazh6Sms4aE8yTDBvVyUyRjJTbFdxTEFVQXdaVjJZRlZ4YmxWUGNFQlhFdEg5MiUyQkRQWGRDTU5RVFh0UU1YZ292VjlqbUxnOUwzbmZKS3JSV2xNS3g2TFVweEpoMTQ5aHd0ZVZBZEY1UlBZTmJV; imm_pl=it; PHPSESSID=d8d48eb1fcac694ff143e88547441403; _gcl_au=1.1.1651193628.1768579902; _gcl_aw=GCL.1772477636.CjwKCAiAh5XNBhAAEiwA_Bu8Fb720_dZTqFsNM8SI1xN0ypZvDY4pQYoCvBfV5ALHicBCHicOG6CABoClBMQAvD_BwE; wp-wpml_current_language=it; _ga_57HD1CXZCY=deleted; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%226ddf3486-5ec7-505b-be44-ca4120061ae0%22%2C%22expiryDate%22%3A%222027-01-16T16%3A40%3A32.942Z%22%7D; didomi_token=eyJ1c2VyX2lkIjoiMTk3OTFhZWQtMzFjNy02ZGU1LTgzZTItYTNiZjdlNWEzNjQ3IiwiY3JlYXRlZCI6IjIwMjUtMTItMjlUMTc6MTQ6MzQuMjMxWiIsInVwZGF0ZWQiOiIyMDI1LTEyLTI5VDE3OjE0OjM5LjQ5OVoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwidHdpdHRlciIsImM6Z29vZ2xlYW5hLTRUWG5KaWdSIiwiYzpsaW5rZWRpbi1tYXJrZXRpbmctc29sdXRpb25zIiwiYzpyZWRkaXQiLCJjOnRpa3Rvay1LWkFVUUxaOSIsImM6bWljcm9zb2Z0IiwiYzpwaW50ZXJlc3QiXX0sInB1cnBvc2VzIjp7ImVuYWJsZWQiOlsiZ2VvbG9jYXRpb25fZGF0YSIsImRldmljZV9jaGFyYWN0ZXJpc3RpY3MiXX0sInZlcnNpb24iOjIsImFjIjoiQ2d5QUdBRmtDS1lLREFBQS5BQUFBIn0=; euconsent-v2=CQdMa0AQdMa0AAHABBENCLFsAP_gAAAAABCYIzQCgAIAAgABUAFsAQgApACzALzAYyBGYAAAApKADAAEF7yEAGAAIL3lIAMAAQXvHQAYAAgveEgAwABBe8AA.f_wAAAAAAAAA; mp_caf0d89293f630a8e27cdb2914e3ef57_mixpanel=%7B%22distinct_id%22%3A%22c4455846-93b7-4db5-bb09-ce5877d71625%22%2C%22%24device_id%22%3A%22c4455846-93b7-4db5-bb09-ce5877d71625%22%2C%22%24initial_referrer%22%3A%22%24direct%22%2C%22%24initial_referring_domain%22%3A%22%24direct%22%7D; IMMSESSID=8c886abfe2a8f657ebe316630fcbf5ff; brazeUserDefined=eyJ2ZXJzaW9uIjoidjciLCJ1c2VyIjoiNmRkZjM0ODYtNWVjNy01MDViLWJlNDQtY2E0MTIwMDYxYWUwIn0="

# Set True to re-fetch details for ALL listings (use when cookie expired and
# all detail data is stale, or on first run after adding many new listings).
FORCE_REFETCH = False

CAPTURE_DATE = "2026-03-15"

# price / size_m2 are the only fields you need to provide here.
# Everything else (rooms, condition, energy class, floor, etc.) is auto-fetched.
# Leave CAPTURED_LISTINGS empty to use auto-discovery (fetches all pages automatically).
# Populate manually only if you want to override or seed specific listings.
CAPTURED_LISTINGS = [
    # Extracted from GA analytics traffic 2026-03-15 (page 1)
    {"id": "126417271", "type": "Appartamento",      "price": 360000, "size_m2": None},
    {"id": "123938287", "type": "Appartamento",      "price": 565000, "size_m2": None},
    {"id": "126226951", "type": "Appartamento",      "price": 199000, "size_m2": None},
    {"id": "125028755", "type": "Appartamento",      "price": 199000, "size_m2": None},
    {"id": "126860785", "type": "Appartamento",      "price": 165000, "size_m2": None},
    {"id": "126452553", "type": "Appartamento",      "price": 255000, "size_m2": None},
    {"id": "124311581", "type": "Appartamento",      "price": 199000, "size_m2": None},
    {"id": "113219009", "type": "Appartamento",      "price": 355000, "size_m2": None},
    {"id": "126987563", "type": "Attico - Mansarda", "price": 319000, "size_m2": None},
    {"id": "124372745", "type": "Appartamento",      "price": 298000, "size_m2": None},
    {"id": "126417579", "type": "Appartamento",      "price": 230000, "size_m2": None},
    {"id": "126996991", "type": "Appartamento",      "price": 480000, "size_m2": None},
    {"id": "125748159", "type": "Attico - Mansarda", "price": 295000, "size_m2": None},
    {"id": "123643709", "type": "Appartamento",      "price": 470000, "size_m2": None},
    {"id": "124360291", "type": "Appartamento",      "price": 289000, "size_m2": None},
    {"id": "127039593", "type": "Appartamento",      "price": 415000, "size_m2": None},
    {"id": "123257495", "type": "Appartamento",      "price": 332000, "size_m2": None},
    {"id": "126491517", "type": "Villa",             "price": 475000, "size_m2": None},
    {"id": "126392493", "type": "Appartamento",      "price": 390000, "size_m2": None},
    {"id": "127253729", "type": "Appartamento",      "price": 395000, "size_m2": None},
    {"id": "127441940", "type": "Appartamento",      "price": 193000, "size_m2": None},
    {"id": "125734121", "type": "Appartamento",      "price": 335000, "size_m2": None},
    {"id": "125717389", "type": "Appartamento",      "price": 239000, "size_m2": None},
    {"id": "125084753", "type": "Appartamento",      "price": 230000, "size_m2": None},
    {"id": "124473643", "type": "Attico - Mansarda", "price": 480000, "size_m2": None},
]
# ===========================================================================

HEADERS_JSON = {
    "accept": "application/json, */*",
    "accept-language": "en-GB,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.3.1 Safari/605.1.15",
    "referer": "https://www.immobiliare.it/vendita-case/peschiera-borromeo/san-bovio-san-felice/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

HEADERS_HTML = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-GB,en;q=0.9",
    "user-agent": HEADERS_JSON["user-agent"],
    "referer": "https://www.immobiliare.it/vendita-case/peschiera-borromeo/san-bovio-san-felice/",
    "cache-control": "max-age=0",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
}

LISTINGS_DIR = Path("peschiera_listings_history")

# -- Auto-discovery: fetch all listings from search page __NEXT_DATA__ -------

def auto_discover_listings():
    """
    Auto-discover all active listings by fetching every search results page and
    parsing __NEXT_DATA__. Handles pagination automatically.
    Returns list of {id, type, price, size_m2} dicts.
    Requires DATADOME_COOKIE to be set.
    """
    if not DATADOME_COOKIE and not BROWSER_COOKIES:
        print("  [auto-discover] No cookies set -- cannot auto-discover listings.")
        return []

    all_listings = []
    page = 1
    while True:
        url = "https://www.immobiliare.it/vendita-case/peschiera-borromeo/san-bovio-san-felice/"
        if page > 1:
            url += f"?pag={page}"
        cookie_str = BROWSER_COOKIES if BROWSER_COOKIES else f"datadome={DATADOME_COOKIE}"
        headers = {**HEADERS_HTML, "cookie": cookie_str}
        try:
            r = requests.get(url, headers=headers, timeout=20)
        except Exception as e:
            print(f"  [auto-discover] Request error page {page}: {e}")
            break

        if r.status_code != 200:
            print(f"  [auto-discover] HTTP {r.status_code} on page {page}")
            break

        nd = _re.search(r'id="__NEXT_DATA__"[^>]*>(.+?)</script>', r.text, _re.DOTALL)
        if not nd:
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
            page_listings.append({"id": lid, "type": typ, "price": price, "size_m2": None})

        all_listings.extend(page_listings)
        print(f"  [auto-discover] Page {page}/{total_pages} -- {len(page_listings)} listings")

        if page >= total_pages or not page_listings:
            break
        page += 1
        time.sleep(random.uniform(15, 35))

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
        floor_str = floor_str[:40].strip()

        # -- Energy class --
        energy_class = None
        energy = p0.get("energy") or {}
        cls = energy.get("class")
        if isinstance(cls, dict):
            energy_class = cls.get("name")
        elif isinstance(cls, str):
            energy_class = cls

        return {
            "surface_m2":     surface,
            "price_verified": price,
            "rooms":          p0.get("rooms"),
            "bathrooms":      p0.get("bathrooms"),
            "condition":      p0.get("condition"),
            "energy_class":   energy_class,
            "floor":          floor_str,
            "title":          re_obj.get("title", "")[:120],
        }
    except Exception:
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
        "?idCategoria=1&idContratto=1&idRegione=lom&idProvincia=MI&idComune=8062&__lang=it"
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
    
    url = "https://www.immobiliare.it/vendita-case/peschiera-borromeo/san-bovio-san-felice/"
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


# -- Main ---------------------------------------------------------------------

def main():
    LISTINGS_DIR.mkdir(exist_ok=True)
    now   = datetime.datetime.now()
    today = now.date()

    print("=" * 62)
    print(f"  Peschiera Borromeo listing monitor  --  {now.strftime('%Y-%m-%d %H:%M')}")
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

    if chart:
        print(f"  Price chart  OK  --  {chart['latest_label']}  EUR{chart['latest_value']:,.0f}/m2")
    else:
        print("  Price chart  FAILED")

    if markers_count is not None:
        live_count = len(active_listings)
        note = ""
        if markers_count != live_count:
            note = f"  <<  markers={markers_count} vs captured={live_count}  -- consider recapturing"
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
        prev_e      = prev_by_id.get(raw["id"], {})
        manual_size = raw.get("size_m2")
        detail      = fetched_details.get(raw["id"]) or {}

        size  = manual_size or detail.get("surface_m2") or prev_e.get("size_m2")
        price = raw.get("price") if raw.get("price") is not None else (
            detail.get("price_verified") or prev_e.get("price")
        )

        entry = {
            "id":            raw["id"],
            "type":          raw["type"],
            "price":         price,
            "size_m2":       size,
            "eur_per_m2":    round(price / size) if (price and size) else None,
            "url":           f"https://www.immobiliare.it/annunci/{raw['id']}/",
            "first_seen":    (
                prev_e.get("first_seen", today.isoformat())
                if raw["id"] in prev_by_id else today.isoformat()
            ),
            "rooms":         detail.get("rooms")        or prev_e.get("rooms"),
            "bathrooms":     detail.get("bathrooms")    or prev_e.get("bathrooms"),
            "condition":     detail.get("condition")    or prev_e.get("condition"),
            "energy_class":  detail.get("energy_class") or prev_e.get("energy_class"),
            "floor":         detail.get("floor")        or prev_e.get("floor"),
            "title":         detail.get("title")        or prev_e.get("title"),
            "detail_fetched": (
                today.isoformat() if detail else prev_e.get("detail_fetched")
            ),
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
    if enriched:
        print("\n" + "-" * 90)
        print("  CURRENT LISTINGS")
        print("-" * 90)
        print(f"  {'#':<3} {'ID':<12} {'Type':<22} {'Price':>12}  {'m2':>5}  {'EUR/m2':>7}  {'Rooms':>5}  {'Cond':<20}  {'Enrg'}  {'DOM':>4}d")
        print(f"  {'-'*3} {'-'*12} {'-'*22} {'-'*12}  {'-'*5}  {'-'*7}  {'-'*5}  {'-'*20}  {'-'*4}  {'-'*5}")
        for i, l in enumerate(enriched, 1):
            dom      = days_on_market(l["first_seen"], today)
            long_note = " LONG" if dom >= 60 else ""
            size_str = f"{l['size_m2']:.0f}" if l.get("size_m2") else "?"
            epm_str  = f"{l['eur_per_m2']:,}" if l.get("eur_per_m2") else "?"
            rooms_str = str(l.get("rooms") or "?")
            cond_str  = (l.get("condition") or "?")[:20]
            enrg_str  = l.get("energy_class") or "?"
            print(f"  {i:<3} {l['id']:<12} {l['type']:<22} {fmt_price(l['price']):>12}  {size_str:>5}  {epm_str:>7}  {rooms_str:>5}  {cond_str:<20}  {enrg_str:<4}  {dom:>4}{long_note}")

        # Appartamenti summary
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
    else:
        print("\n  No listings captured yet.")
        if not DATADOME_COOKIE:
            print("  Set DATADOME_COOKIE to enable auto-discovery, or populate CAPTURED_LISTINGS manually.")
        else:
            print("  Auto-discovery found nothing -- check if the cookie is fresh.")

    # Market index (last 13 months)
    if chart:
        print("\n" + "-" * 62)
        print("  MARKET INDEX  (immobiliare.it asking EUR/m2, Peschiera Borromeo)")
        print("-" * 62)
        for lbl, val in chart["history"][-13:]:
            bar = "#" * int(val / 100)
            print(f"  {lbl:<10}  {val:>7,.0f}  {bar}")

    # Snapshot history
    path = save_snapshot(snapshot, now)
    all_snaps = list_snapshots()
    print("\n" + "-" * 62)
    print(f"  Saved  ->  {path.name}")
    print(f"  All snapshots in {LISTINGS_DIR}/  ({len(all_snaps)} total):")
    for s in all_snaps:
        snap = load_snapshot(s)
        cnt  = len(snap.get("listings", []))
        print(f"    {s.name}   ({cnt} listings)")
    print("=" * 62)


if __name__ == "__main__":
    main()
