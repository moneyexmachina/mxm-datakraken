"""
Shared typing utilities for mxm-datakraken.

This module defines `JSONLike`, a recursive type alias representing any value that
can be serialized to (or deserialized from) JSON using Python's `json` module.

- `JSONScalar` covers primitive JSON values.
- `JSONLike` allows nested lists and dicts with string keys and JSON-like values.

Examples
--------
Valid:
    {"isin": "IE00B4L5Y983", "weights": [0.1, 0.2, 0.7], "meta": {"active": True}}

Invalid (non-string dict keys, non-JSON types):
    {1: "x"}              # keys must be str
    {"dt": datetime.now()}  # datetime is not JSON-serializable by default
"""

from __future__ import annotations

from typing import TypeAlias

JSONScalar: TypeAlias = str | int | float | bool | None
JSONLike: TypeAlias = JSONScalar | list["JSONLike"] | dict[str, "JSONLike"]

__all__ = ["JSONScalar", "JSONLike"]
