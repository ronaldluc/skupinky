#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[2]
V2_DIR = ROOT / 'v2'
DEFAULT_SEED = V2_DIR / 'data' / 'seed_ds.json'
WORKER_SCRIPT = V2_DIR / 'scraping' / 'extract_obsazenost_batch_worker.mjs'
OUT_BASE = V2_DIR / 'scraping' / 'out'


@dataclass
class BatchJob:
  batch_id: int
  batch_file: Path
  run_dir: Path
  force_rewrite: bool
  strict_pick: bool
  headful: bool
  step_sleep_ms: int
  skip_existing_dir: Path | None


def split_into_batches(items: list[dict], num_batches: int) -> list[list[dict]]:
  if num_batches <= 0:
    raise ValueError('num_batches must be > 0')
  if not items:
    return []

  bucket_count = min(num_batches, len(items))
  buckets: list[list[dict]] = [[] for _ in range(bucket_count)]
  for idx, item in enumerate(items):
    buckets[idx % bucket_count].append(item)
  return [b for b in buckets if b]


def _worker_run(job: BatchJob) -> dict:
  env = os.environ.copy()
  env['BATCH_FILE'] = str(job.batch_file)
  env['BATCH_ID'] = str(job.batch_id)
  env['RUN_DIR'] = str(job.run_dir)
  env['STEP_SLEEP_MS'] = str(job.step_sleep_ms)
  env['FORCE_REWRITE'] = '1' if job.force_rewrite else '0'
  env['STRICT_PICK'] = '1' if job.strict_pick else '0'
  env['HEADFUL'] = '1' if job.headful else '0'
  if job.skip_existing_dir is not None:
    env['SKIP_EXISTING_DIR'] = str(job.skip_existing_dir)

  proc = subprocess.run(
    ['node', str(WORKER_SCRIPT)],
    cwd=str(ROOT),
    env=env,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
  )

  result = {
    'batch_id': job.batch_id,
    'returncode': proc.returncode,
    'stdout': proc.stdout.strip(),
    'stderr': proc.stderr.strip(),
  }

  if proc.returncode == 0:
    try:
      result['meta'] = json.loads(proc.stdout.strip().splitlines()[-1]) if proc.stdout.strip() else {}
    except Exception:
      result['meta'] = {}
  return result


def _load_json_if_exists(path: Path, fallback: Iterable | dict):
  if path.exists():
    return json.loads(path.read_text(encoding='utf-8'))
  return fallback


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(description='Parallel scraper runner for v2 pipeline')
  parser.add_argument('--seed-file', type=Path, default=DEFAULT_SEED)
  parser.add_argument('--batches', type=int, default=60, help='Number of seed batches (default: 60)')
  parser.add_argument('--workers', type=int, default=max(1, mp.cpu_count() // 2))
  parser.add_argument('--max-items', type=int, default=0, help='Only process first N seed records')
  parser.add_argument('--run-name', type=str, default='')
  parser.add_argument('--force-rewrite', action='store_true')
  parser.add_argument('--strict-pick', action='store_true')
  parser.add_argument('--headful', action='store_true')
  parser.add_argument('--step-sleep-ms', type=int, default=100)
  parser.add_argument('--clean-run-dir', action='store_true', help='Delete run dir before start')
  parser.add_argument(
    '--skip-existing-dir',
    type=Path,
    default=V2_DIR / 'data' / 'all_ds',
    help='Skip scraping DS whose target CSV already exists in this directory (default: v2/data/all_ds)',
  )
  return parser.parse_args()


def main() -> int:
  args = parse_args()

  if not args.seed_file.exists():
    print(f'Seed file does not exist: {args.seed_file}', file=sys.stderr)
    return 2
  if not WORKER_SCRIPT.exists():
    print(f'Worker script missing: {WORKER_SCRIPT}', file=sys.stderr)
    return 2
  if args.batches <= 0:
    print('--batches must be > 0', file=sys.stderr)
    return 2
  if args.workers <= 0:
    print('--workers must be > 0', file=sys.stderr)
    return 2
  if args.skip_existing_dir and not args.skip_existing_dir.exists():
    args.skip_existing_dir.mkdir(parents=True, exist_ok=True)

  seed = json.loads(args.seed_file.read_text(encoding='utf-8'))
  if args.max_items and args.max_items > 0:
    seed = seed[: args.max_items]

  if not seed:
    print('No seed entries to process.')
    return 0

  run_name = args.run_name or datetime.now().strftime('run_%Y%m%d_%H%M%S')
  run_dir = OUT_BASE / run_name
  batches_dir = run_dir / 'batches'
  state_dir = run_dir / 'state'
  all_dir = run_dir / 'all_ds'

  if args.clean_run_dir and run_dir.exists():
    shutil.rmtree(run_dir)

  batches_dir.mkdir(parents=True, exist_ok=True)
  state_dir.mkdir(parents=True, exist_ok=True)
  all_dir.mkdir(parents=True, exist_ok=True)

  batches = split_into_batches(seed, args.batches)

  jobs: List[BatchJob] = []
  for batch_id, batch_items in enumerate(batches):
    batch_file = batches_dir / f'batch_{batch_id:04d}.json'
    batch_file.write_text(json.dumps(batch_items, ensure_ascii=False, indent=2), encoding='utf-8')
    jobs.append(
      BatchJob(
        batch_id=batch_id,
        batch_file=batch_file,
        run_dir=run_dir,
        force_rewrite=args.force_rewrite,
        strict_pick=args.strict_pick,
        headful=args.headful,
        step_sleep_ms=args.step_sleep_ms,
        skip_existing_dir=args.skip_existing_dir,
      )
    )

  worker_count = min(args.workers, len(jobs))

  print(
    json.dumps(
      {
        'seed_count': len(seed),
        'batch_count': len(jobs),
        'workers': worker_count,
        'run_dir': str(run_dir),
      },
      ensure_ascii=False,
    )
  )

  failed_batches: list[dict] = []
  metas: list[dict] = []

  with mp.Pool(processes=worker_count) as pool:
    for result in tqdm(
      pool.imap_unordered(_worker_run, jobs),
      total=len(jobs),
      desc='Batch progress',
      unit='batch',
    ):
      if result['returncode'] != 0:
        failed_batches.append(result)
      else:
        metas.append(result.get('meta', {}))

  all_done: list[dict] = []
  all_errors: list[dict] = []
  for batch_id in range(len(jobs)):
    done_file = state_dir / f'batch_{batch_id}_done.json'
    err_file = state_dir / f'batch_{batch_id}_errors.json'
    all_done.extend(_load_json_if_exists(done_file, []))
    all_errors.extend(_load_json_if_exists(err_file, []))

  summary = {
    'run_name': run_name,
    'created_at': datetime.now().isoformat(),
    'seed_count': len(seed),
    'batch_count': len(jobs),
    'workers': worker_count,
    'completed_entries': len(all_done),
    'error_entries': len(all_errors),
    'failed_batches': [
      {
        'batch_id': b['batch_id'],
        'returncode': b['returncode'],
        'stderr_tail': '\n'.join((b.get('stderr') or '').splitlines()[-20:]),
      }
      for b in failed_batches
    ],
    'meta_count': len(metas),
    'output_dir': str(run_dir),
  }

  (run_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
  (run_dir / 'done_all.json').write_text(json.dumps(all_done, ensure_ascii=False, indent=2), encoding='utf-8')
  (run_dir / 'errors_all.json').write_text(json.dumps(all_errors, ensure_ascii=False, indent=2), encoding='utf-8')

  print(json.dumps(summary, ensure_ascii=False, indent=2))

  return 1 if failed_batches else 0


if __name__ == '__main__':
  raise SystemExit(main())
