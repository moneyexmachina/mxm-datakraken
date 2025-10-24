from __future__ import annotations

from pathlib import Path
from typing import Callable, cast

import pytest
from mxm_config import load_config
from omegaconf import DictConfig
from omegaconf.errors import ReadonlyConfigError

from mxm_datakraken.config.config import (
    justetf_dataio_view,
    justetf_http_adapter_view,
    justetf_view,
)


def _load_cfg_from_repo_yaml(
    mxm_config_home: Callable[[str, str], Path],
    *,
    env: str = "dev",
    profile: str = "default",
) -> DictConfig:
    # Mirror mxm_datakraken/config/*.yaml into MXM_CONFIG_HOME/mxm-datakraken/
    mxm_config_home("mxm-datakraken", "mxm_datakraken")
    cfg = cast(
        DictConfig, load_config(package="mxm-datakraken", env=env, profile=profile)
    )
    assert isinstance(cfg, DictConfig)
    return cfg


def test_justetf_view_has_core_paths_and_is_readonly(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg = _load_cfg_from_repo_yaml(mxm_config_home, env="dev", profile="default")
    view = cast(DictConfig, justetf_view(cfg))  # resolve=True by default

    # Core keys exist and are non-empty strings
    required_top = (
        "root",
        "profile_index_dir",
        "profiles_dir",
        "parsed_dir",
        "logs_dir",
    )
    for key in required_top:
        assert key in view
        val = view[key]
        assert isinstance(val, str) and len(val) > 0

    # Nested dataio paths present
    assert "dataio" in view
    assert isinstance(view.dataio.paths.root, str)  # type: ignore[attr-defined]
    assert isinstance(view.dataio.paths.db_path, str)  # type: ignore[attr-defined]
    assert isinstance(view.dataio.paths.responses_dir, str)  # type: ignore[attr-defined]

    # Composition includes env/profile suffixes (sanity)
    assert "/dev/datakraken/default" in view.root  # type: ignore[attr-defined]
    assert view.root.endswith("/sources/justetf")  # type: ignore[attr-defined]
    assert view.parsed_dir.endswith("/sources/justetf/parsed")  # type: ignore[attr-defined]

    # Read-only enforced
    with pytest.raises(ReadonlyConfigError):
        view.root = "/tmp/override"  # type: ignore[attr-defined]


def test_justetf_dataio_view_paths_and_env_cache(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg_dev = _load_cfg_from_repo_yaml(mxm_config_home, env="dev", profile="default")
    cfg_prod = _load_cfg_from_repo_yaml(mxm_config_home, env="prod", profile="default")

    dview_dev = cast(DictConfig, justetf_dataio_view(cfg_dev))
    dview_prod = cast(DictConfig, justetf_dataio_view(cfg_prod))

    # Paths exist
    assert isinstance(dview_dev.paths.root, str) and dview_dev.paths.root  # type: ignore[attr-defined]
    assert isinstance(dview_dev.paths.db_path, str) and dview_dev.paths.db_path  # type: ignore[attr-defined]
    assert (
        isinstance(dview_dev.paths.responses_dir, str) and dview_dev.paths.responses_dir
    )  # type: ignore[attr-defined]

    # Env override for cache.use_cache (dev True, prod False)
    # If you didn't set env overrides, relax these assertions.
    assert bool(getattr(getattr(dview_dev, "cache", {}), "use_cache", True)) is True  # type: ignore[attr-defined]
    assert bool(getattr(getattr(dview_prod, "cache", {}), "use_cache", True)) is False  # type: ignore[attr-defined]

    # Read-only enforced
    with pytest.raises(ReadonlyConfigError):
        dview_dev.paths.db_path = "/tmp/x.sqlite"  # type: ignore[attr-defined]


def test_profile_overlay_changes_paths_for_research(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg = _load_cfg_from_repo_yaml(mxm_config_home, env="dev", profile="research")
    pview = cast(DictConfig, justetf_view(cfg))
    # From profile.yaml override: parsed_dir → parsed_research
    assert pview.parsed_dir.endswith("/sources/justetf/parsed_research")  # type: ignore[attr-defined]


def test_justetf_http_adapter_view_defaults_and_readonly(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg = _load_cfg_from_repo_yaml(mxm_config_home, env="dev", profile="default")

    # View should exist and be read-only
    aview = cast(DictConfig, justetf_http_adapter_view(cfg))  # resolve=True by default
    assert isinstance(aview, DictConfig)

    # Core fields exist with sane types
    assert "enabled" in aview
    assert isinstance(aview.enabled, bool)  # type: ignore[attr-defined]

    assert "alias" in aview
    assert isinstance(aview.alias, str) and len(aview.alias) > 0  # type: ignore[attr-defined]

    assert "user_agent" in aview
    assert isinstance(aview.user_agent, str) and "mxm-datakraken" in aview.user_agent  # type: ignore[attr-defined]

    assert "default_timeout" in aview
    assert (
        isinstance(aview.default_timeout, (int, float))
        and float(aview.default_timeout) > 0.0
    )  # type: ignore[attr-defined]

    # Headers (if configured) should be a mapping with string values
    headers = getattr(aview, "default_headers", None)  # type: ignore[attr-defined]
    if headers is not None:
        # OmegaConf allows both dot and item access; use item access for keys like "Accept"
        assert "Accept" in headers
        assert isinstance(headers["Accept"], str)

    # Read-only enforced
    with pytest.raises(ReadonlyConfigError):
        aview.alias = "override-me"  # type: ignore[attr-defined]
