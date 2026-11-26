"""
Run the full pipeline locally without Airflow or Docker.

    python scripts/run_local.py             # live FakeStore API
    python scripts/run_local.py --offline   # bundled JSON fixtures (no network)
    python scripts/run_local.py --skip-dbt  # stop after the DuckDB load

Then:  streamlit run dashboard.py
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from include.config import DBT_PROJECT_DIR, dbt_executable, duckdb_path  # noqa: E402
from include.pipeline import run_pipeline  # noqa: E402

FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures" / "fakestore"


def _find_dbt() -> str:
    candidate = dbt_executable()
    if shutil.which(candidate):
        return candidate
    # Fall back to the dbt installed next to the current interpreter (an un-activated venv).
    scripts_dir = Path(sys.executable).parent
    for name in ("dbt", "dbt.exe"):
        if (scripts_dir / name).exists():
            return str(scripts_dir / name)
    raise SystemExit("dbt not found: pip install -r requirements-dev.txt")


def run_dbt() -> None:
    env = {**os.environ, "DUCKDB_PATH": str(duckdb_path().resolve())}
    cmd = [_find_dbt(), "build", "--project-dir", str(DBT_PROJECT_DIR)]
    cmd += ["--profiles-dir", str(DBT_PROJECT_DIR)]
    subprocess.run(cmd, check=True, env=env)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--offline", action="store_true", help="use bundled JSON fixtures")
    parser.add_argument("--skip-dbt", action="store_true", help="do not run dbt build")
    args = parser.parse_args()

    started = time.perf_counter()
    summary = run_pipeline(fixtures_dir=FIXTURES_DIR if args.offline else None)
    if not args.skip_dbt:
        run_dbt()
    summary["seconds"] = round(time.perf_counter() - started, 2)
    summary["warehouse"] = str(duckdb_path())
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
