# v2 — SDS Stats: Dostupnost dětských skupin v ČR

Interactive map showing estimated free capacity in Czech child-care groups (dětské skupiny / sousedské dětské skupiny) based on open data from MPSV.

## Prerequisites

- Python 3.10+ with `pandas`, `tqdm`
- Node.js 20+ (for webapp and scraping worker)
- Playwright (`npx playwright install chromium`) — only for scraping

## Quick start

```bash
make install      # install webapp npm dependencies
make preprocess   # compute open spots + build frontend JSON
make dev          # start Vite dev server on localhost:6007
```

## Full pipeline (includes scraping)

```bash
make install
make scrape       # prepare seed → run parallel Playwright scraper
make preprocess   # compute open spots → build web data + geocode
make deploy       # preprocess → build → deploy placeholder
```

## Status logic (v2)

Status is derived from **Mon–Fri free spots only** (weekends excluded):

| Status | Condition | Legend |
|--------|-----------|-------|
| **green** | `min(free Mon–Fri) >= 1` | Volné místo každý pracovní den |
| **orange** | `max(free Mon–Fri) > 0` but `min < 1` | Volné některým dnem |
| **red** | `max(free Mon–Fri) == 0` | Pravděpodobně plně obsazeno |
| **unknown** | No data | Nedostatek dat |

Output fields: `free_min` (minimum over Mon–Fri), `free_max` (maximum over Mon–Fri).

## Directory layout

```
v2/
├── Makefile
├── scraping/
│   ├── prepare_seed.py
│   ├── run_scrape_parallel.py
│   └── extract_obsazenost_batch_worker.mjs
├── preprocessing/
│   ├── compute_open_spots.py
│   └── build_web_data.py
├── data/
│   ├── overview/   (symlink to ../../overview_data/)
│   ├── seed_ds.json
│   └── all_ds/     (1104 scraped CSVs)
├── summary/        (output of compute_open_spots.py)
└── webapp/
    ├── package.json
    ├── vite.config.ts
    ├── index.html
    ├── src/
    │   ├── main.tsx
    │   ├── App.tsx
    │   └── index.css
    └── public/data/
        ├── ds_points.json
        ├── kraj_stats.json
        ├── kraje.geojson
        └── geocode_cache.json
```

## Data sources

- **MPSV open data**: registry of child-care groups (overview CSV)
- **MPSV Power BI portal**: occupancy data (scraped via Playwright)
- **Nominatim / OpenStreetMap**: geocoding
- **OpenFreeMap**: map tiles
