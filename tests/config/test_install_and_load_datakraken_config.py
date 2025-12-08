from __future__ import annotations

from pathlib import Path

import pytest
from mxm.config import DefaultsMode, install_config, load_config

APP_ID = "datakraken"
SHIPPED_PACKAGE = "mxm.datakraken"


def _install_datakraken_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Install datakraken shipped config into a temporary MXM_CONFIG_HOME.
    """
    monkeypatch.setenv("MXM_CONFIG_HOME", str(tmp_path))

    install_config(
        app_id=APP_ID,
        mode=DefaultsMode.shipped,
        shipped_package=SHIPPED_PACKAGE,
    )


def test_dev_bridge_default_profile_structure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    env=dev, machine=bridge, profile=default
    - paths resolve correctly via machine + env + profile
    - justetf paths interpolate correctly
    - environment (dev) sets use_cache = True
    """
    _install_datakraken_config(tmp_path, monkeypatch)

    cfg = load_config(
        package=APP_ID,
        env="dev",
        machine="bridge",
        profile="default",
    )

    # Attribute existence (not membership)
    assert hasattr(cfg, "paths")
    assert hasattr(cfg, "sources")
    assert hasattr(cfg.sources, "justetf")

    # Machine + env + profile interpolation
    expected_root = "/Users/mxm/mxm-data/dev/datakraken/default"
    assert cfg.paths.data_root == expected_root

    # Derived justetf root
    assert cfg.sources.justetf.root == f"{expected_root}/sources/justetf"

    # From environment.yaml: dev → use_cache = true
    assert cfg.sources.justetf.dataio.cache.use_cache is True


def test_prod_bridge_cache_off(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    env=prod should set use_cache = False.
    """
    _install_datakraken_config(tmp_path, monkeypatch)

    cfg = load_config(
        package=APP_ID,
        env="prod",
        machine="bridge",
        profile="default",
    )

    assert cfg.sources.justetf.dataio.cache.use_cache is False


def test_research_profile_overrides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    profile=research should apply overrides from profile.yaml.
    """
    _install_datakraken_config(tmp_path, monkeypatch)

    cfg = load_config(
        package=APP_ID,
        env="dev",
        machine="bridge",
        profile="research",
    )

    expected_root = "/Users/mxm/mxm-data/dev/datakraken/research"
    assert cfg.paths.data_root == expected_root

    # sources.justetf.root = <data_root>/sources/justetf
    expected_justetf_root = f"{expected_root}/sources/justetf"
    assert cfg.sources.justetf.root == expected_justetf_root

    # From profile.yaml: parsed_dir -> ${sources.justetf.root}/parsed_research
    assert cfg.sources.justetf.parsed_dir == f"{expected_justetf_root}/parsed_research"

    # From profile.yaml: responses_dir -> ${sources.justetf.root}/responses_research
    responses_dir = cfg.sources.justetf.dataio.paths.responses_dir
    assert responses_dir == f"{expected_justetf_root}/responses_research"
