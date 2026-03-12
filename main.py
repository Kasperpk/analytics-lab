"""
analytics-lab pipeline runner
==============================
Single entry-point that orchestrates data extraction and dbt transformations.

Usage:
    uv run pipeline                 # runs the Eurostat pipeline (default)
    uv run pipeline eurostat        # same as above
    uv run pipeline ecommerce       # runs the ecommerce pipeline
    uv run pipeline all             # runs every pipeline
"""

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DBT_DIR = ROOT / "dbt" / "analytics_project"


# ── helpers ────────────────────────────────────────────────────────────────────

def _find_dbt() -> str:
    """Locate the dbt executable."""
    dbt = shutil.which("dbt")
    if dbt:
        return dbt
    # Fallback: look in the same venv bin/Scripts directory as the running Python
    venv_bin = Path(sys.executable).parent
    for name in ("dbt", "dbt.exe"):
        candidate = venv_bin / name
        if candidate.exists():
            return str(candidate)
    sys.exit("ERROR: dbt not found on PATH. Run 'uv sync' first.")


def _run(cmd: list[str], *, cwd: Path | None = None, label: str = "") -> bool:
    """Run a command, stream output, return True on success."""
    print(f"\n{'-' * 60}")
    print(f">> {label or ' '.join(cmd)}")
    print(f"{'-' * 60}")
    result = subprocess.run(cmd, cwd=cwd)
    if result.returncode != 0:
        print(f"\n[FAIL] (exit {result.returncode}): {label or ' '.join(cmd)}")
    return result.returncode == 0


def _dbt(args: list[str], *, label: str = "") -> bool:
    """Run a dbt command inside the dbt project directory."""
    dbt = _find_dbt()
    return _run(
        [dbt] + args + ["--profiles-dir", "."],
        cwd=DBT_DIR,
        label=label or f"dbt {' '.join(args)}",
    )


# ── pipelines ──────────────────────────────────────────────────────────────────

def run_eurostat() -> bool:
    """Extract Eurostat data → build bronze/silver/gold models → run tests."""
    print(f"\n{'=' * 60}")
    print("    Eurostat Macroeconomic Analytics Pipeline")
    print(f"{'=' * 60}")

    t0 = time.time()
    steps: list[tuple[str, bool]] = []

    # 1. Extract data from Eurostat API
    ok = _run(
        [sys.executable, str(ROOT / "ingestion" / "eurostat_ingest.py")],
        cwd=ROOT,
        label="Extract Eurostat datasets",
    )
    steps.append(("Extract", ok))
    if not ok:
        _summary(steps, t0)
        return False

    # 2. Install dbt packages (idempotent — fast if already installed)
    ok = _dbt(["deps"], label="Install dbt packages")
    steps.append(("dbt deps", ok))
    if not ok:
        _summary(steps, t0)
        return False

    # 3. Build Eurostat models and run tests
    ok = _dbt(
        ["build", "--select", "staging.eurostat+", "marts.eurostat+"],
        label="Build & test Eurostat models",
    )
    steps.append(("dbt build", ok))

    _summary(steps, t0)
    return ok


def run_ecommerce() -> bool:
    """Build ecommerce bronze/silver/gold models → run tests."""
    print(f"\n{'=' * 60}")
    print("    E-commerce Analytics Pipeline")
    print(f"{'=' * 60}")

    t0 = time.time()
    steps: list[tuple[str, bool]] = []

    ok = _dbt(["deps"], label="Install dbt packages")
    steps.append(("dbt deps", ok))
    if not ok:
        _summary(steps, t0)
        return False

    ok = _dbt(
        ["build", "--select", "staging.ecommerce+", "marts.ecommerce+"],
        label="Build & test ecommerce models",
    )
    steps.append(("dbt build", ok))

    _summary(steps, t0)
    return ok


def run_all() -> bool:
    """Run every pipeline in sequence."""
    ok_euro = run_eurostat()
    ok_ecom = run_ecommerce()
    return ok_euro and ok_ecom


def _summary(steps: list[tuple[str, bool]], t0: float) -> None:
    """Print a compact run summary."""
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print("  Pipeline Summary")
    print(f"{'-' * 60}")
    for name, ok in steps:
        icon = "[OK]" if ok else "[FAIL]"
        print(f"  {icon}  {name}")
    print(f"{'-' * 60}")
    all_ok = all(ok for _, ok in steps)
    status = "PASSED" if all_ok else "FAILED"
    print(f"  Result: {status}  ({elapsed:.1f}s)")
    print(f"{'=' * 60}\n")


# ── CLI ────────────────────────────────────────────────────────────────────────

PIPELINES = {
    "eurostat": run_eurostat,
    "ecommerce": run_ecommerce,
    "all": run_all,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="Run analytics-lab data pipelines.",
    )
    parser.add_argument(
        "pipeline",
        nargs="?",
        default="eurostat",
        choices=PIPELINES.keys(),
        help="Which pipeline to run (default: eurostat).",
    )
    args = parser.parse_args()

    runner = PIPELINES[args.pipeline]
    success = runner()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
