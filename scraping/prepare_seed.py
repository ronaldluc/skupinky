import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OVERVIEW = ROOT / 'v2' / 'data' / 'overview' / 'detske_skupiny_evidendce_poskytovatelu_data.csv'
OUT_DIR = ROOT / 'v2' / 'data'
SEED_FILE = OUT_DIR / 'seed_ds.json'


def clean(s):
    if s is None:
        return ''
    s = str(s)
    s = s.replace('\u00a0', ' ').replace('\u2007', ' ').replace('\u202f', ' ')
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def maybe_website(provider_text: str):
    m = re.search(r'(https?://\S+|www\.\S+)', provider_text or '', flags=re.I)
    if not m:
        return None
    u = m.group(1).rstrip('.,;)')
    if not u.startswith('http'):
        u = 'https://' + u
    return u


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(OVERVIEW, sep=';', encoding='utf-8-sig')
    df = df[df['stav_opravneni_ds'].fillna('').str.contains('Aktiv', case=False)]

    rows = []
    for i, r in df.reset_index(drop=True).iterrows():
        ds = clean(r.get('nazev_ds'))
        provider_full = clean(r.get('poskytovatel_udaje'))
        rows.append({
            'index': i + 1,
            'kod_detske_skupiny': clean(r.get('kod_detske_skupiny')),
            'nazev_ds': ds,
            'nazev_poskytovatele': clean(r.get('nazev_poskytovatele')),
            'provider_full': provider_full,
            'ico_poskytovatele': clean(r.get('ico_poskytovatele')),
            'misto_poskytovani_ds': clean(r.get('misto_poskytovani_ds')),
            'obec_ds': clean(r.get('obec_ds')),
            'orp_ds': clean(r.get('orp_ds')),
            'kraj_ds': clean(r.get('kraj_ds')),
            'kapacita_ds': None if pd.isna(pd.to_numeric(r.get('kapacita_ds'), errors='coerce')) else int(pd.to_numeric(r.get('kapacita_ds'), errors='coerce')),
            'is_sds': 'sousedská' in ds.lower(),
            'website_url': maybe_website(provider_full),
        })

    with open(SEED_FILE, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(json.dumps({'seed_count': len(rows), 'output': str(SEED_FILE)}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
