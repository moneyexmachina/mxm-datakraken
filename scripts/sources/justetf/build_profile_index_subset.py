"""
Create a filtered JustETF profile index limited to a predefined ETF universe.

Workflow:
1) Load layered config (mxm-config).
2) Register DataIO adapters (bootstrap).
3) Ensure the full JustETF profile index exists (network/cache via API).
4) Resolve the *latest* profile_index bucket date.
5) Resolve ISIN universe:
    - CLI override path (--universe-path), else
    - cfg.sources.justetf.etf_universe_override_paths (optional list), else
    - packaged default from mxm_datakraken.assets.etf_universe
6) Filter index entries to those ISINs.
7) Persist filtered index under:
     profile_index_subsets/<YYYY-MM-DD>/{subset.parsed.json, subset.meta.json}
   and refresh `profile_index_subsets/latest -> <YYYY-MM-DD>`.
"""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence, cast

from mxm_config import MXMConfig, load_config

from mxm_datakraken.assets.etf_universe import (
    load_default_isin_universe,
    load_isin_universe_override,
)
from mxm_datakraken.bootstrap import register_adapters_from_config
from mxm_datakraken.common.file_io import read_json, write_json
from mxm_datakraken.common.latest_bucket import resolve_latest_bucket
from mxm_datakraken.common.types import JSONLike
from mxm_datakraken.sources.justetf.common.models import ETFProfileIndexEntry
from mxm_datakraken.sources.justetf.profile_index.api import get_profile_index


def _sha256_lines(lines: Iterable[str]) -> str:
    h = hashlib.sha256()
    for line in lines:
        h.update(line.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def filter_index_by_isins(
    index: Sequence[ETFProfileIndexEntry], isins: Sequence[str]
) -> list[ETFProfileIndexEntry]:
    """Return only entries whose ISIN is in the provided list."""
    isins_set = set(isins)
    subset = [entry for entry in index if entry.get("isin") in isins_set]
    print(f"Filtered {len(subset)} of {len(index)} total entries.")
    return subset


def _ensure_symlink(link_path: Path, target_rel: Path) -> None:
    """
    Create/replace a symlink at `link_path` pointing to `target_rel`.
    Removes any existing file/dir/symlink first.
    """
    if link_path.exists() or link_path.is_symlink():
        if link_path.is_dir() and not link_path.is_symlink():
            for p in sorted(link_path.rglob("*"), reverse=True):
                if p.is_file() or p.is_symlink():
                    p.unlink()
                elif p.is_dir():
                    p.rmdir()
            link_path.rmdir()
        else:
            link_path.unlink()
    link_path.symlink_to(target_rel)


@dataclass(frozen=True)
class SubsetMeta:
    generated_at_utc: str
    source_bucket: str
    universe_count: int
    universe_hash: str
    subset_count: int
    notes: str | None = None


def _resolve_universe_isins(
    *, cfg: MXMConfig, cli_universe_path: str | None
) -> list[str]:
    """
    Resolution order:
      1) CLI override path (if provided)
      2) cfg.sources.justetf.etf_universe_override_paths (optional list[str])
      3) packaged default dataset
    """
    candidates: list[str] = []
    if cli_universe_path:
        candidates.append(cli_universe_path)

    # Best-effort read of config override paths without assuming strict structure.
    try:
        # Expecting something like a list[str], but be defensive:
        maybe_paths = getattr(cfg.sources.justetf, "etf_universe_override_paths", None)  # type: ignore[attr-defined]
        if isinstance(maybe_paths, (list, tuple)):
            candidates.extend([str(p) for p in maybe_paths])
    except Exception:
        pass

    override = load_isin_universe_override(candidates)
    if override is not None:
        print(f"Loaded {len(override)} ISINs from override path(s).")
        return _normalize_isins(override)

    # Fall back to packaged default
    packaged = load_default_isin_universe()
    print(f"Loaded {len(packaged)} ISINs from packaged default universe.")
    return _normalize_isins(packaged)


def _normalize_isins(isins: Sequence[str]) -> list[str]:
    """Upper-case, strip, and de-duplicate while preserving order."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in isins:
        s = raw.strip().upper()
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def save_subset_index(
    *,
    subset: Sequence[ETFProfileIndexEntry],
    base_path: Path,
    source_bucket: str,
    universe_isins: Sequence[str],
) -> Path:
    """Save the filtered subset into a bucket-mirrored directory with provenance."""
    root = base_path / "profile_index_subsets"
    bucket_dir = root / source_bucket
    bucket_dir.mkdir(parents=True, exist_ok=True)

    subset_path = bucket_dir / "subset.parsed.json"
    meta_path = bucket_dir / "subset.meta.json"
    latest_link = root / "latest"

    # Write subset
    write_json(subset_path, cast(JSONLike, list(subset)))

    # Meta/provenance
    meta = SubsetMeta(
        generated_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        source_bucket=source_bucket,
        universe_count=len(universe_isins),
        universe_hash=_sha256_lines(universe_isins),
        subset_count=len(subset),
        notes=None,
    )
    write_json(meta_path, asdict(meta))

    # Refresh latest symlink
    _ensure_symlink(latest_link, Path(source_bucket))

    # Optional convenience copy at repo root (comment out if not desired)
    convenience = base_path / "profile_index_subsets_subset_latest.json"
    write_json(convenience, read_json(subset_path))

    print(
        "Saved subset snapshot to:\n"
        f"  - {subset_path}\n"
        f"  - {meta_path}\n"
        f"  - {latest_link} -> {source_bucket}\n"
        f"  - {convenience} (convenience copy)"
    )
    return subset_path


def main(
    *, force_refresh: bool, env: str, profile: str, universe_path: str | None
) -> None:
    """Main entry point for building a subset profile index."""
    # 1) Load config and register adapters (DataIO)
    cfg = load_config("mxm-datakraken", env=env, profile=profile)
    register_adapters_from_config(cfg)

    # 2) Resolve base path (source-local root)
    base_path = Path(cfg.sources.justetf.root)

    # 3) Universe (override -> packaged)
    universe_isins = _resolve_universe_isins(cfg=cfg, cli_universe_path=universe_path)
    print(f"Universe contains {len(universe_isins)} ISINs.")

    # 4) Ensure/obtain the full profile index
    index: list[ETFProfileIndexEntry] = get_profile_index(
        cfg=cfg, base_path=base_path, force_refresh=force_refresh
    )
    print(f"Profile index contains {len(index)} total entries.")

    # 5) Resolve the *bucket* we just used for determinism in storage
    source_bucket = resolve_latest_bucket(base_path / "profile_index")
    print(f"Resolved source bucket: {source_bucket}")

    # 6) Filter + 7) Save under profile_index_subsets/<bucket>/
    subset = filter_index_by_isins(index, universe_isins)
    save_subset_index(
        subset=subset,
        base_path=base_path,
        source_bucket=source_bucket,
        universe_isins=universe_isins,
    )

    print("✅ Done. Subset profile index ready for downstream use.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Build a bucketed JustETF profile index subset "
            "for a predefined ISIN universe."
        )
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help=(
            "Force rebuild of the full profile index from sitemap"
            " (ignore cached latest)."
        ),
    )
    parser.add_argument("--env", default=None, help="mxm-config environment override.")
    parser.add_argument("--profile", default=None, help="mxm-config profile override.")
    parser.add_argument(
        "--universe-path",
        default=None,
        help=(
            "Optional path to a newline-delimited ISIN list to"
            " override the packaged default."
        ),
    )
    args = parser.parse_args()

    # Provide defaults so we never pass Optional[str] to load_config
    main(
        force_refresh=bool(args.force_refresh),
        env=args.env or "dev",
        profile=args.profile or "default",
        universe_path=args.universe_path,
    )
