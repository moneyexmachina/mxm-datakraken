"""
inspect_snapshot.py

Quick CLI to inspect the latest JustETF snapshot.

Functions:
1. List all ISINs in the current snapshot.
2. Pretty-print a specific ETF profile (by ISIN or index).

Usage examples:
    poetry run python scripts/sources/justetf/inspect_snapshot.py --list
    poetry run python scripts/sources/justetf/inspect_snapshot.py --isin IE00B4L5Y983
    poetry run python scripts/sources/justetf/inspect_snapshot.py --index 5
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Sequence, cast

from mxm_config import load_config
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from mxm_datakraken.config.config import justetf_view
from mxm_datakraken.sources.justetf.common.models import JustETFProfile

console = Console()


def _as_profile_list(obj: object) -> list[JustETFProfile]:
    """Best-effort shape check and cast to list[JustETFProfile]."""
    if not isinstance(obj, list):
        raise ValueError("Snapshot JSON must be a list")
    out: list[JustETFProfile] = []
    for i, item in enumerate(obj):
        if not isinstance(item, dict):
            raise ValueError(f"Snapshot entry #{i} is not an object")
        # Minimal validation: require 'isin'
        if "isin" not in item or not isinstance(item["isin"], str):
            raise ValueError(f"Snapshot entry #{i} missing 'isin' (str)")
        out.append(cast(JustETFProfile, item))
    return out


def load_latest_snapshot(base_path: Path) -> list[JustETFProfile]:
    """Load the most recent profiles snapshot file."""
    profiles_dir = base_path / "profiles"
    snapshots = sorted(profiles_dir.glob("profiles_*.json"))
    if not snapshots:
        raise FileNotFoundError(f"No profiles_*.json found in {profiles_dir}")
    latest = snapshots[-1]
    raw = json.loads(latest.read_text(encoding="utf-8"))
    data = _as_profile_list(raw)
    console.print(f"[cyan]Loaded snapshot:[/cyan] {latest.name} ({len(data)} entries)")
    return data


def list_isins(snapshot: Sequence[JustETFProfile]) -> None:
    """Print all ISINs in the snapshot."""
    table = Table(title="ETF ISINs in Snapshot")
    table.add_column("Index", justify="right")
    table.add_column("ISIN", style="bold cyan")
    table.add_column("Name", style="white")
    for i, entry in enumerate(snapshot):
        isin = entry.get("isin", "")
        name = entry.get("name", "")
        table.add_row(str(i), isin, name)
    console.print(table)


def show_etf_profile(
    snapshot: Sequence[JustETFProfile],
    *,
    isin: str | None = None,
    index: int | None = None,
) -> None:
    """Pretty-print a single ETF profile."""
    entry: JustETFProfile | None = None

    if isinstance(isin, str) and isin:
        entry = next((e for e in snapshot if e.get("isin") == isin), None)
    elif isinstance(index, int) and 0 <= index < len(snapshot):
        entry = snapshot[index]

    if not entry:
        console.print(f"[red]ETF not found (isin={isin}, index={index})[/red]")
        return

    # Header
    name = entry.get("name", "(no name)")
    isin_val = entry.get("isin", "(no isin)")
    header = f"[bold cyan]{name}[/bold cyan]\n[white]ISIN:[/white] {isin_val}"
    console.print(Panel(header, title="ETF Profile", subtitle=entry.get("source_url")))

    # Data section (key data table)
    data_table = Table(title="Fund Data")
    data_table.add_column("Field", style="cyan")
    data_table.add_column("Value", style="white")

    data_map = cast(Mapping[str, str] | None, entry.get("data"))
    if data_map:
        for k, v in data_map.items():
            data_table.add_row(k, v)
    else:
        data_table.add_row("(no fields)", "")
    console.print(data_table)

    # Listings section
    listings = cast(
        Sequence[Mapping[str, str]] | None,
        entry.get("listings") or entry.get("Listings"),
    )

    if listings and len(listings) > 0:
        first = listings[0]
        headers = list(first.keys())
        list_table = Table(title="Listings")
        for h in headers:
            list_table.add_column(h, style="cyan")
        for row in listings:
            # Row may be Mapping; make a stable lookup sequence
            list_table.add_row(*(row.get(h, "") for h in headers))
        console.print(list_table)
    else:
        console.print("[yellow]No listings found in profile.[/yellow]")


def main() -> None:
    cfg = load_config("mxm-datakraken", env="dev", profile="default")

    # Robust to layout changes: use the JustETF view
    base_path = Path(justetf_view(cfg).root)  # type: ignore[attr-defined]

    snapshot = load_latest_snapshot(base_path)

    parser = argparse.ArgumentParser(description="Inspect the latest JustETF snapshot.")
    parser.add_argument(
        "--list", action="store_true", help="List all ISINs in the snapshot."
    )
    parser.add_argument("--isin", type=str, help="Show details for a given ISIN.")
    parser.add_argument(
        "--index", type=int, help="Show details for a profile at given index."
    )
    args = parser.parse_args()

    if args.list:
        list_isins(snapshot)
    elif args.isin or args.index is not None:
        show_etf_profile(snapshot, isin=args.isin, index=args.index)
    else:
        console.print(
            "[yellow]Use --list to view all ISINs, or --isin/--index to inspect one.[/yellow]"
        )


if __name__ == "__main__":
    main()
