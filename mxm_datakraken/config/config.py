"""
Config helpers for mxm-datakraken.

This module translates the app config into the minimal shape that mxm-dataio
expects for JustETF, and centralizes where we read the adapter alias from.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # type-only import; no runtime dependency here
    from omegaconf import DictConfig


def dataio_for_justetf(cfg: "DictConfig") -> tuple[str, dict[str, Any]]:
    """
    Return (adapter_alias, dataio_cfg) for JustETF.

    Required (mxm-datakraken default.yaml):
      paths.sources.justetf.root: str
      paths.sources.justetf.dataio.db_path: str
      paths.sources.justetf.dataio.responses_dir: str
    Optional:
      paths.sources.justetf.http.alias: str (defaults to "http")
    """
    try:
        j = cfg.paths.sources.justetf
        root = str(j.root)
        db_path = str(j.dataio.db_path)
        responses_dir = str(j.dataio.responses_dir)
        alias = str(getattr(getattr(j, "http", None), "alias", "http"))
    except Exception as e:
        # Fail fast with a precise message; this is a programming/config error
        raise ValueError(
            "justetf config incomplete. Expected "
            "paths.sources.justetf.{root, dataio.{db_path,responses_dir}, http.alias?}"
        ) from e

    if not root or not db_path or not responses_dir:
        raise ValueError(
            "justetf config requires non-empty "
            "paths.sources.justetf.root and dataio.{db_path,responses_dir}"
        )

    dataio_cfg: dict[str, Any] = {
        "paths": {
            "data_root": root,
            "db_path": db_path,  # required by mxm-dataio
            "responses_dir": responses_dir,  # required by mxm-dataio
        }
    }
    return alias or "http", dataio_cfg
