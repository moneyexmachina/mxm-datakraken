"""
Config views for mxm-datakraken.

- justetf_view(cfg):          filesystem + source-level settings for JustETF
- justetf_dataio_view(cfg):   DataIO subtree for JustETF (pass to DataIoSession)
- dataio_for_justetf(cfg):    convenience tuple (source_name, dataio_view)
- justetf_http_adapter_view(cfg): Http adapter config
"""

from __future__ import annotations

from typing import Any, Iterable

from mxm_config import MXMConfig, make_view

SOURCE_JUSTETF = "justetf"


def justetf_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """Read-only view rooted at `sources.justetf`."""
    return make_view(cfg, "sources.justetf", resolve=resolve)


def justetf_dataio_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """Read-only view rooted at `sources.justetf.dataio` (for DataIoSession/Store)."""
    return make_view(cfg, "sources.justetf.dataio", resolve=resolve)


def dataio_for_justetf(
    cfg: MXMConfig, *, resolve: bool = True
) -> tuple[str, MXMConfig]:
    """Convenience: return (source_name, dataio_view) for JustETF."""
    return SOURCE_JUSTETF, justetf_dataio_view(cfg, resolve=resolve)


def justetf_http_adapter_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """
    Read-only view rooted at `sources.justetf.dataio.adapters.http`.
    """
    return make_view(cfg, "sources.justetf.dataio.adapters.http", resolve=resolve)


class ConfigError(RuntimeError):
    pass


def _must_have(d: Any, path: str, keys: Iterable[str]) -> None:
    missing = [k for k in keys if not hasattr(d, k)]
    if missing:
        raise ConfigError(f"Missing keys at {path}: {', '.join(missing)}")


def ensure_justetf_config(cfg: MXMConfig) -> None:
    # works for DictConfig or wrapper; views already do the right thing after your _root_cfg shim
    j = justetf_view(cfg)
    _must_have(
        j,
        "sources.justetf",
        ("root", "profile_index_dir", "profiles_dir", "parsed_dir", "logs_dir"),
    )

    dio = justetf_dataio_view(cfg)
    _must_have(dio, "sources.justetf.dataio.paths", ("paths",))
    _must_have(
        dio.paths, "sources.justetf.dataio.paths", ("root", "db_path", "responses_dir")
    )

    http = justetf_http_adapter_view(cfg)
    _must_have(
        http,
        "sources.justetf.dataio.adapters.http",
        ("enabled", "alias", "user_agent", "default_timeout"),
    )


__all__ = [
    "SOURCE_JUSTETF",
    "justetf_view",
    "justetf_dataio_view",
    "dataio_for_justetf",
    "justetf_http_adapter_view",
    "ensure_justetf_config",
]
