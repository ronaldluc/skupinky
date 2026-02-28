.PHONY: install seed scrape preprocess dev build deploy clean

PYTHON ?= python3
NPM    ?= npm

# ── Install webapp dependencies ──────────────────────
install:
	cd webapp && $(NPM) install

# ── Scraping pipeline ────────────────────────────────
seed:
	$(PYTHON) scraping/prepare_seed.py

scrape: seed
	$(PYTHON) scraping/run_scrape_parallel.py

# ── Preprocessing pipeline ───────────────────────────
preprocess:
	$(PYTHON) preprocessing/compute_open_spots.py
	$(PYTHON) preprocessing/build_web_data.py

# ── Development server ───────────────────────────────
dev:
	cd webapp && $(NPM) run dev

# ── Production build ─────────────────────────────────
build: preprocess
	cd webapp && $(NPM) run build

# ── Deploy (placeholder) ─────────────────────────────
deploy: build
	@echo "Deploy webapp/dist/ to your hosting provider (Vercel, Netlify, etc.)"

# ── Cleanup ──────────────────────────────────────────
clean:
	rm -rf summary/*.csv summary/*.json
	rm -rf webapp/public/data/ds_points.json webapp/public/data/kraj_stats.json webapp/public/data/kraje.geojson webapp/public/data/build_report.json
