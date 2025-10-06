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

import json
from pathlib import Path

from mxm_config import load_config
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def load_latest_snapshot(base_path: Path) -> list[dict]:
    """Load the most recent profiles snapshot file."""
    profiles_dir = base_path / "profiles"
    snapshots = sorted(profiles_dir.glob("profiles_*.json"))
    if not snapshots:
        raise FileNotFoundError(f"No profiles_*.json found in {profiles_dir}")
    latest = snapshots[-1]
    data = json.loads(latest.read_text(encoding="utf-8"))
    console.print(f"[cyan]Loaded snapshot:[/cyan] {latest.name} ({len(data)} entries)")
    return data


def list_isins(snapshot: list[dict]) -> None:
    """Print all ISINs in the snapshot."""
    table = Table(title="ETF ISINs in Snapshot")
    table.add_column("Index", justify="right")
    table.add_column("ISIN", style="bold cyan")
    table.add_column("Name", style="white")
    for i, entry in enumerate(snapshot):
        table.add_row(str(i), entry.get("isin", ""), entry.get("name", ""))
    console.print(table)


def show_etf_profile(
    snapshot: list[dict], isin: str | None = None, index: int | None = None
) -> None:
    """Pretty-print a single ETF profile."""
    entry = None
    if isin:
        entry = next((e for e in snapshot if e.get("isin") == isin), None)
    elif index is not None and 0 <= index < len(snapshot):
        entry = snapshot[index]

    if not entry:
        console.print(f"[red]ETF not found (isin={isin}, index={index})[/red]")
        return

    # Basic info
    header = (
        f"[bold cyan]{entry['name']}[/bold cyan]\n[white]ISIN:[/white] {entry['isin']}"
    )
    console.print(Panel(header, title="ETF Profile", subtitle=entry.get("source_url")))

    # Data section
    data_table = Table(title="Fund Data")
    data_table.add_column("Field", style="cyan")
    data_table.add_column("Value", style="white")
    for k, v in entry.get("data", {}).items():
        data_table.add_row(k, v)
    console.print(data_table)
    # Listings
    listings = entry.get("listings") or entry.get("Listings") or []

    if listings:
        list_table = Table(title="Listings")
        # Derive columns dynamically from first listing
        headers = list(listings[0].keys())
        for h in headers:
            list_table.add_column(h, style="cyan")

        for l in listings:
            list_table.add_row(*(l.get(h, "") for h in headers))

        console.print(list_table)
    else:
        console.print("[yellow]No listings found in profile.[/yellow]")


def main():
    cfg = load_config("mxm-datakraken", env="dev", profile="default")
    base_path = Path(cfg.paths.sources.justetf.root)
    snapshot = load_latest_snapshot(base_path)

    import argparse

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
