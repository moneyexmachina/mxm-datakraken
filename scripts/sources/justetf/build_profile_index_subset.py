"""
build_profile_index_subset.py

Create a filtered JustETF profile index limited to a predefined ETF universe.

Workflow:
1. Load layered config (mxm-config).
2. Register DataIO adapters (bootstrap).
3. Load or build the full JustETF profile index (sitemap-based, via DataIO).
4. Read `etf_universe_ISINs.txt` from the JustETF source root.
5. Filter index entries to those ISINs.
6. Persist filtered index as dated and `subset_latest.json`.

This script does NOT download or parse individual ETF profiles.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Sequence

from mxm_config import load_config

from mxm_datakraken.bootstrap import register_adapters_from_config
from mxm_datakraken.sources.justetf.common.models import ETFProfileIndexEntry
from mxm_datakraken.sources.justetf.profile_index.api import get_profile_index


def read_universe_file(universe_path: Path) -> list[str]:
    """Read newline-delimited ISINs from text file."""
    if not universe_path.exists():
        raise FileNotFoundError(f"Universe file not found: {universe_path}")
    return [
        line.strip().upper()
        for line in universe_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def filter_index_by_isins(
    index: Sequence[ETFProfileIndexEntry], isins: Sequence[str]
) -> list[ETFProfileIndexEntry]:
    """Return only entries whose ISIN is in the provided list."""
    isins_set = set(isins)
    subset = [entry for entry in index if entry.get("isin") in isins_set]
    print(f"Filtered {len(subset)} of {len(index)} total entries.")
    return subset


def save_subset_index(subset: Sequence[ETFProfileIndexEntry], base_path: Path) -> Path:
    """Save the filtered subset as both dated and latest snapshots."""
    subset_dir = base_path / "profile_index"
    subset_dir.mkdir(parents=True, exist_ok=True)

    today = date.today()
    dated_path = subset_dir / f"profile_index_subset_{today.isoformat()}.json"
    latest_path = subset_dir / "subset_latest.json"

    payload = json.dumps(list(subset), ensure_ascii=False, indent=2)
    dated_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")

    print(f"Saved subset snapshot to:\n  - {dated_path}\n  - {latest_path}")
    return latest_path


def main(
    *, force_refresh: bool = False, env: str = "dev", profile: str = "default"
) -> None:
    """Main entry point for building a subset profile index."""
    # 1) Load config and register adapters (DataIO)
    cfg = load_config("mxm-datakraken", env=env, profile=profile)
    register_adapters_from_config(cfg)

    # 2) Resolve base path (source-local root)
    base_path = Path(cfg.paths.sources.justetf.root)

    # 3) Load ETF universe ISIN list
    universe_path = base_path / "etf_universe_ISINs.txt"
    isins = read_universe_file(universe_path)
    print(f"Loaded {len(isins)} ISINs from universe file.")

    # 4) Get or build the full profile index
    index: list[ETFProfileIndexEntry] = get_profile_index(
        cfg=cfg, base_path=base_path, force_refresh=force_refresh
    )
    print(f"Profile index contains {len(index)} total entries.")

    # 5) Filter and save subset
    subset = filter_index_by_isins(index, isins)
    save_subset_index(subset, base_path)

    print("✅ Done. Subset profile index ready for downstream use.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build a filtered JustETF profile index subset."
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force rebuild of the full profile index from sitemap (ignore cached latest.json).",
    )
    parser.add_argument(
        "--env", default=None, help="mxm-config environment override (optional)."
    )
    parser.add_argument(
        "--profile", default=None, help="mxm-config profile override (optional)."
    )
    args = parser.parse_args()
    # Provide defaults so we never pass Optional[str] to load_config
    main(
        force_refresh=bool(args.force_refresh),
        env=args.env or "dev",
        profile=args.profile or "default",
    )
