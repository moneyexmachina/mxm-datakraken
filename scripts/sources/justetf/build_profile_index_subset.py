"""
build_profile_index_subset.py

Create a filtered JustETF profile index limited to a predefined ETF universe.

Workflow:
1. Resolve data paths via mxm-config.
2. Load or build the full JustETF profile index (sitemap-based).
3. Read `etf_universe_ISINs.txt` from the JustETF source root.
4. Filter index entries to those ISINs.
5. Persist filtered index as dated and `subset_latest.json`.

This script does NOT download or parse individual ETF profiles.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from mxm_config import load_config

from mxm_datakraken.sources.justetf.profile_index.api import get_profile_index


def read_universe_file(universe_path: Path) -> list[str]:
    """Read newline-delimited ISINs from text file."""
    if not universe_path.exists():
        raise FileNotFoundError(f"Universe file not found: {universe_path}")
    isin_list = [
        line.strip().upper()
        for line in universe_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return isin_list


def filter_index_by_isins(index: list[dict], isins: list[str]) -> list[dict]:
    """Return only entries whose ISIN is in the provided list."""
    isins_set = set(isins)
    subset = [entry for entry in index if entry.get("isin") in isins_set]
    print(f"Filtered {len(subset)} of {len(index)} total entries.")
    return subset


def save_subset_index(subset: list[dict], base_path: Path) -> Path:
    """Save the filtered subset as both dated and latest snapshots."""
    subset_dir = base_path / "profile_index"
    subset_dir.mkdir(parents=True, exist_ok=True)

    today = date.today()
    dated_path = subset_dir / f"profile_index_subset_{today.isoformat()}.json"
    latest_path = subset_dir / "subset_latest.json"

    for path in (dated_path, latest_path):
        with path.open("w", encoding="utf-8") as f:
            json.dump(subset, f, ensure_ascii=False, indent=2)

    print(f"Saved subset snapshot to:\n  - {dated_path}\n  - {latest_path}")
    return latest_path


def main(force_refresh: bool = False) -> None:
    """Main entry point for building a subset profile index."""
    # 1. Load layered config (machine/env/profile aware)
    cfg = load_config("mxm-datakraken", env="dev", profile="default")
    base_path = Path(cfg.paths.sources.justetf.root)

    # 2. Load ETF universe ISIN list
    universe_path = base_path / "etf_universe_ISINs.txt"
    isins = read_universe_file(universe_path)
    print(f"Loaded {len(isins)} ISINs from universe file.")

    # 3. Get or build the full profile index
    index = get_profile_index(base_path=base_path, force_refresh=force_refresh)
    print(f"Profile index contains {len(index)} total entries.")

    # 4. Filter and save subset
    subset = filter_index_by_isins(index, isins)
    save_subset_index(subset, base_path)

    print("✅ Done. Subset profile index ready for downstream use.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Build a filtered JustETF profile index subset."
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force rebuild of the full profile index from sitemap (ignore cached latest.json).",
    )
    args = parser.parse_args()
    main(force_refresh=args.force_refresh)
