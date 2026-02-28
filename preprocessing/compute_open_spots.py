import glob
import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SEED_FILE = ROOT / 'v2' / 'data' / 'seed_ds.json'
RAW_DIR = ROOT / 'v2' / 'data' / 'all_ds'
SUMMARY_DIR = ROOT / 'v2' / 'summary'

DAYS = ['Pondělí', 'Úterý', 'Středa', 'Čtvrtek', 'Pátek', 'Sobota', 'Neděle']


def parse_idx(path: str):
    m = re.match(r'^(\d+)_', Path(path).name)
    if not m:
        return None
    return int(m.group(1))


def read_seed():
    with open(SEED_FILE, 'r', encoding='utf-8') as f:
        arr = json.load(f)
    return {int(x['index']): x for x in arr}


def analyze_file(csv_path: str, seed_item: dict):
    capacity = seed_item.get('kapacita_ds')
    df = pd.read_csv(csv_path)
    has_data = len(df) > 0

    max_by_day = {}
    if has_data:
        hour_cols = [c for c in df.columns if c not in ['den', 'datum', 'orientační počet volných míst']]
        for c in hour_cols:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        df['pocet_deti_den'] = df[hour_cols].max(axis=1)
        max_by_day = df.groupby('den')['pocet_deti_den'].max().to_dict()

    rows = []
    for day in DAYS:
        max_occ = max_by_day.get(day)
        free = None
        never_full = None
        if capacity is not None and max_occ is not None:
            free = max(0.0, float(capacity) - float(max_occ))
            never_full = bool(max_occ < capacity)

        rows.append({
            'index': seed_item['index'],
            'kod_detske_skupiny': seed_item.get('kod_detske_skupiny'),
            'ds': seed_item.get('nazev_ds'),
            'provider': seed_item.get('nazev_poskytovatele'),
            'is_sds': bool(seed_item.get('is_sds')),
            'misto_poskytovani_ds': seed_item.get('misto_poskytovani_ds'),
            'kapacita_ds': capacity,
            'den': day,
            'max_obsazenost_za_mesic_v_dni': None if max_occ is None else float(max_occ),
            'odhad_volnych_mist': free,
            'nikdy_plne_v_tomto_dni': never_full,
            'zdroj_data_dostupny': has_data,
        })

    sat = next((x for x in rows if x['den'] == 'Sobota'), None)
    sun = next((x for x in rows if x['den'] == 'Neděle'), None)
    weekend_active = False
    weekend_free_sum = None
    if sat and sun:
        sat_occ = sat['max_obsazenost_za_mesic_v_dni']
        sun_occ = sun['max_obsazenost_za_mesic_v_dni']
        weekend_active = bool((sat_occ is not None and sat_occ > 0) or (sun_occ is not None and sun_occ > 0))
        if weekend_active and sat['odhad_volnych_mist'] is not None and sun['odhad_volnych_mist'] is not None:
            weekend_free_sum = float(sat['odhad_volnych_mist']) + float(sun['odhad_volnych_mist'])

    summary = {
        'index': seed_item['index'],
        'kod_detske_skupiny': seed_item.get('kod_detske_skupiny'),
        'ds': seed_item.get('nazev_ds'),
        'provider': seed_item.get('nazev_poskytovatele'),
        'is_sds': bool(seed_item.get('is_sds')),
        'kapacita_ds': capacity,
        'zdroj_data_dostupny': has_data,
        'weekend_active': weekend_active,
        'vikend_odhad_volnych_mist_sobota_plus_nedele': weekend_free_sum,
    }

    return rows, summary


def main():
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    seed = read_seed()

    daily_rows = []
    summary_rows = []

    files = sorted(glob.glob(str(RAW_DIR / '*.csv')))
    for fp in files:
        idx = parse_idx(fp)
        if idx is None or idx not in seed:
            continue
        rows, summary = analyze_file(fp, seed[idx])
        daily_rows.extend(rows)
        summary_rows.append(summary)

    daily_df = pd.DataFrame(daily_rows)
    summary_df = pd.DataFrame(summary_rows)

    daily_csv = SUMMARY_DIR / 'open_spots_by_ds_day.csv'
    summary_csv = SUMMARY_DIR / 'open_spots_by_ds_summary.csv'
    daily_df.to_csv(daily_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)

    nested = {}
    for _, r in daily_df.iterrows():
        key = str(r['index'])
        if key not in nested:
            nested[key] = {
                'index': int(r['index']),
                'ds': r['ds'],
                'provider': r['provider'],
                'is_sds': bool(r['is_sds']),
                'kapacita_ds': None if pd.isna(r['kapacita_ds']) else int(r['kapacita_ds']),
                'days': {},
            }
        nested[key]['days'][r['den']] = {
            'max_obsazenost_za_mesic_v_dni': None if pd.isna(r['max_obsazenost_za_mesic_v_dni']) else float(r['max_obsazenost_za_mesic_v_dni']),
            'odhad_volnych_mist': None if pd.isna(r['odhad_volnych_mist']) else float(r['odhad_volnych_mist']),
            'nikdy_plne_v_tomto_dni': None if pd.isna(r['nikdy_plne_v_tomto_dni']) else bool(r['nikdy_plne_v_tomto_dni']),
        }

    with open(SUMMARY_DIR / 'open_spots_by_ds_day.json', 'w', encoding='utf-8') as f:
        json.dump(nested, f, ensure_ascii=False, indent=2)

    coverage = {
        'seed_ds_total': len(seed),
        'csv_files_processed': len(files),
        'unique_ds_processed': int(summary_df['index'].nunique() if len(summary_df) else 0),
        'with_nonempty_obsazenost': int(summary_df['zdroj_data_dostupny'].sum() if len(summary_df) else 0),
        'sds_count_processed': int(summary_df['is_sds'].sum() if len(summary_df) else 0),
    }
    with open(SUMMARY_DIR / 'open_spots_coverage.json', 'w', encoding='utf-8') as f:
        json.dump(coverage, f, ensure_ascii=False, indent=2)

    print(json.dumps(coverage, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
