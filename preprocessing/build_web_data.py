import json
import os
import re
import time
import urllib.request
from pathlib import Path
from urllib.parse import quote

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OVERVIEW = ROOT / 'v2' / 'data' / 'overview' / 'detske_skupiny_evidendce_poskytovatelu_data.csv'
SEED_FILE = ROOT / 'v2' / 'data' / 'seed_ds.json'
OPEN_SPOTS = ROOT / 'v2' / 'summary' / 'open_spots_by_ds_day.csv'
OUT = ROOT / 'v2' / 'webapp' / 'public' / 'data'
CACHE_FILE = OUT / 'geocode_cache.json'
KRAJE_URL = 'https://raw.githubusercontent.com/siwekm/czech-geojson/master/kraje.json'

WORKDAYS = ['Pondělí', 'Úterý', 'Středa', 'Čtvrtek', 'Pátek']


def normalize(s: str) -> str:
    s = '' if s is None else str(s)
    s = s.replace('\u00a0', ' ').replace('\u2007', ' ').replace('\u202f', ' ')
    return re.sub(r'\s+', ' ', s).strip()


def nominatim_lookup(query: str):
    url = f'https://nominatim.openstreetmap.org/search?format=jsonv2&countrycodes=cz&limit=1&q={quote(query)}'
    req = urllib.request.Request(
        url,
        headers={
            'User-Agent': 'sds-ds-v2-map-build/1.0',
            'Accept-Language': 'cs',
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        rows = json.load(resp)
    if not rows:
        return None
    return float(rows[0]['lon']), float(rows[0]['lat'])


def geocode(cache: dict, address: str, obec: str, kraj: str):
    address = normalize(address)
    obec = normalize(obec)
    kraj = normalize(kraj)

    queries = []
    if address:
        queries.append(f'{address}, Česká republika')
    if obec and kraj:
        queries.append(f'{obec}, {kraj}, Česká republika')
    if obec:
        queries.append(f'{obec}, Česká republika')
    if kraj:
        queries.append(f'{kraj}, Česká republika')

    for q in queries:
        k = f'q::{q.lower()}'
        if k in cache:
            val = cache[k]
            if val is None:
                continue
            return float(val['lon']), float(val['lat']), True
        try:
            val = nominatim_lookup(q)
            cache[k] = None if val is None else {'lon': val[0], 'lat': val[1]}
            time.sleep(1.05)
            if val is not None:
                return val[0], val[1], False
        except Exception:
            cache[k] = None
            time.sleep(1.05)
    return None, None, False


def derive_status(day_rows):
    """V2 status logic: only Mon-Fri days determine status.

    - green:   min(free over Mon-Fri) >= 1  (free spot every working day)
    - orange:  max(free over Mon-Fri) > 0 but min < 1  (free some days)
    - red:     max(free over Mon-Fri) == 0  (no free spots)
    - unknown: no data
    """
    if not day_rows:
        return 'unknown', None, None, False

    sat = day_rows.get('Sobota', {}).get('max_obsazenost')
    sun = day_rows.get('Neděle', {}).get('max_obsazenost')
    weekend_active = (sat is not None and sat > 0) or (sun is not None and sun > 0)

    frees = []
    for d in WORKDAYS:
        free = day_rows.get(d, {}).get('free')
        if free is not None:
            frees.append(float(free))

    if not frees:
        return 'unknown', None, None, weekend_active

    free_min = min(frees)
    free_max = max(frees)

    if free_min >= 1:
        return 'green', free_min, free_max, weekend_active
    if free_max > 0:
        return 'orange', free_min, free_max, weekend_active
    return 'red', 0.0, 0.0, weekend_active


def maybe_website(provider_text: str):
    m = re.search(r'(https?://\S+|www\.\S+)', provider_text or '', flags=re.I)
    if not m:
        return None
    u = m.group(1).rstrip('.,;)')
    if not u.startswith('http'):
        u = 'https://' + u
    return u


def main():
    OUT.mkdir(parents=True, exist_ok=True)

    seed = json.loads(Path(SEED_FILE).read_text(encoding='utf-8'))
    seed_by_index = {int(x['index']): x for x in seed}

    overview = pd.read_csv(OVERVIEW, sep=';', encoding='utf-8-sig')
    overview = overview[overview['stav_opravneni_ds'].fillna('').str.contains('Aktiv', case=False)]
    ov_by_code = {}
    for _, r in overview.iterrows():
        code = normalize(r.get('kod_detske_skupiny'))
        if code:
            ov_by_code[code] = r

    day_df = pd.read_csv(OPEN_SPOTS) if OPEN_SPOTS.exists() else pd.DataFrame()
    avail = {}
    for _, r in day_df.iterrows():
        idx = int(r.get('index'))
        day = r.get('den')
        avail.setdefault(idx, {})[day] = {
            'free': None if pd.isna(r.get('odhad_volnych_mist')) else float(r.get('odhad_volnych_mist')),
            'max_obsazenost': None if pd.isna(r.get('max_obsazenost_za_mesic_v_dni')) else float(r.get('max_obsazenost_za_mesic_v_dni')),
        }

    cache = json.loads(CACHE_FILE.read_text(encoding='utf-8')) if CACHE_FILE.exists() else {}
    max_new = os.getenv('GEOCODE_MAX_NEW')
    max_new = None if not max_new else int(max_new)
    new_count = 0

    points = []
    skipped = 0
    for idx, item in seed_by_index.items():
        day_rows = avail.get(idx, {})
        if not day_rows:
            continue
        status, free_min, free_max, weekend_active = derive_status(day_rows)

        code = normalize(item.get('kod_detske_skupiny'))
        ov = ov_by_code.get(code)

        address = normalize(item.get('misto_poskytovani_ds') or (ov.get('misto_poskytovani_ds') if ov is not None else ''))
        obec = normalize(item.get('obec_ds') or (ov.get('obec_ds') if ov is not None else ''))
        kraj = normalize(item.get('kraj_ds') or (ov.get('kraj_ds') if ov is not None else ''))

        lon, lat, from_cache = geocode(cache, address, obec, kraj)
        if not from_cache and lon is not None and lat is not None:
            new_count += 1
            if max_new is not None and new_count >= max_new:
                break

        if lon is None or lat is None:
            skipped += 1
            continue

        free_by_day = {}
        for d in ['Pondělí', 'Úterý', 'Středa', 'Čtvrtek', 'Pátek', 'Sobota', 'Neděle']:
            free = day_rows.get(d, {}).get('free')
            if d in ['Sobota', 'Neděle'] and not weekend_active:
                free_by_day[d] = None
            else:
                free_by_day[d] = None if free is None else float(free)

        points.append({
            'id': str(item.get('kod_detske_skupiny') or idx),
            'index': idx,
            'name': normalize(item.get('nazev_ds')),
            'provider': normalize(item.get('nazev_poskytovatele')),
            'provider_full': normalize(item.get('provider_full')),
            'is_sds': bool(item.get('is_sds')),
            'address': address,
            'obec': obec,
            'okres': normalize(item.get('orp_ds')),
            'kraj': kraj,
            'capacity': item.get('kapacita_ds'),
            'status': status,
            'free_min': free_min,
            'free_max': free_max,
            'weekend_active': weekend_active,
            'free_by_day': free_by_day,
            'website_url': item.get('website_url') or maybe_website(item.get('provider_full')),
            'search_query': f"{normalize(item.get('nazev_ds'))} {address}".strip(),
            'lon': lon,
            'lat': lat,
        })

    kraj_stats = {}
    for p in points:
        k = p['kraj'] or 'Neznámý kraj'
        s = kraj_stats.setdefault(k, {
            'kraj': k,
            'ds_count_total': 0,
            'capacity_total': 0,
            'free_min_total': 0.0,
            'green': 0,
            'orange': 0,
            'red': 0,
            'unknown': 0,
        })
        s['ds_count_total'] += 1
        if p['capacity'] is not None:
            s['capacity_total'] += int(p['capacity'])
        if p['free_min'] is not None:
            s['free_min_total'] += float(p['free_min'])
        s[p['status']] += 1

    geo = json.load(urllib.request.urlopen(KRAJE_URL))

    (OUT / 'ds_points.json').write_text(json.dumps(points, ensure_ascii=False), encoding='utf-8')
    (OUT / 'kraj_stats.json').write_text(json.dumps(list(kraj_stats.values()), ensure_ascii=False), encoding='utf-8')
    (OUT / 'kraje.geojson').write_text(json.dumps(geo, ensure_ascii=False), encoding='utf-8')
    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False), encoding='utf-8')

    report = {
        'points_written': len(points),
        'kraj_stats_written': len(kraj_stats),
        'skipped_ungeocoded': skipped,
        'new_geocoded': new_count,
    }
    (OUT / 'build_report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
