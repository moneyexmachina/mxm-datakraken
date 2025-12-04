"""
Read a Markdown (or any text) file and extract all valid ISINs.
- Uses strict regex for ISIN shape: 2 letters + 9 alphanumerics + 1 digit
- Validates each match with the ISO 6166 (Luhn) checksum
- De-duplicates and sorts results by default

Usage:
  python extract_isins.py /path/to/file.md
  python extract_isins.py /path/to/file.md -o /path/to/isin_list.txt
  python extract_isins.py /path/to/file.md --no-sort --no-unique
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Set

ISIN_REGEX = re.compile(r"\b[A-Z]{2}[A-Z0-9]{9}[0-9]\b")


def isin_valid(isin: str) -> bool:
    """
    Validate ISIN using ISO 6166 (Luhn).
    Steps:
      1) Expand letters to digits (A=10 ... Z=35)
      2) Apply Luhn mod-10 to the entire expanded string
    """
    expanded = []
    for ch in isin:
        if ch.isalpha():
            expanded.append(str(ord(ch) - 55))  # 'A'->10 ... 'Z'->35
        else:
            expanded.append(ch)
    digits = "".join(expanded)

    total = 0
    # Right-to-left; double every second digit (0-based index after reverse)
    for i, dch in enumerate(reversed(digits)):
        d = int(dch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def find_isins_in_text(text: str) -> List[str]:
    """Return all regex matches (may include duplicates)."""
    return ISIN_REGEX.findall(text)


def validate_isins(isins: Iterable[str]) -> List[str]:
    """Filter only valid ISINs by checksum."""
    return [i for i in isins if isin_valid(i)]


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    """De-duplicate while preserving first-seen order."""
    seen: Set[str] = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract valid ISINs from a Markdown file."
    )
    parser.add_argument("path", type=Path, help="Path to the .md (or text) file.")
    parser.add_argument(
        "-o", "--out", type=Path, default=None, help="Optional output file path."
    )
    parser.add_argument(
        "--no-unique", action="store_true", help="Do not de-duplicate results."
    )
    parser.add_argument("--no-sort", action="store_true", help="Do not sort results.")
    args = parser.parse_args()

    text = args.path.read_text(encoding="utf-8")

    candidates = find_isins_in_text(text)
    valid = validate_isins(candidates)

    if not args.no_unique:
        valid = unique_preserve_order(valid)

    if not args.no_sort:
        valid = sorted(valid)

    output = "\n".join(valid)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(output + ("\n" if output else ""), encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    main()
