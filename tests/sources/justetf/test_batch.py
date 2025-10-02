"""
Tests for batch orchestration (profile_index → fetch missing profiles → snapshot).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mxm_datakraken.sources.justetf.batch import run_batch


@pytest.fixture
def fake_profile_dict() -> dict[str, str]:
    return {
        "isin": "DUMMY",
        "name": "Dummy Fund",
        "description": "Test fund",
        "data": {},
    }


def test_run_batch_downloads_and_snapshots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake_profile_dict: dict[str, str],
) -> None:
    """Runs end-to-end with two ETFs, forcing download, and creates a snapshot."""

    # Fake profile index returns two ETFs
    index_entries = [
        {"isin": "IE00AAA11111", "url": "http://dummy/etf1"},
        {"isin": "IE00BBB22222", "url": "http://dummy/etf2"},
    ]

    def fake_get_profile_index(*args, **kwargs):
        return index_entries

    def fake_download_html(isin: str, url: str, timeout: int = 30) -> str:
        return "<html>dummy</html>"

    def fake_parse_profile(html: str, isin: str):
        profile = fake_profile_dict.copy()
        profile["isin"] = isin
        return profile

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.get_profile_index", fake_get_profile_index
    )
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.download_etf_profile_html",
        fake_download_html,
    )
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.parse_profile", fake_parse_profile
    )

    snapshot_path = run_batch(tmp_path, rate_seconds=0.0, force=True, run_id="testrun")

    # Snapshot exists and has 2 entries
    assert snapshot_path.exists()
    content = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert isinstance(content, list)
    assert len(content) == 2

    # Progress log has 2 "ok" statuses
    progress_lines = (
        (tmp_path / "profiles" / "runs" / "testrun" / "progress.jsonl")
        .read_text(encoding="utf-8")
        .strip()
        .splitlines()
    )
    statuses = [json.loads(line)["status"] for line in progress_lines]
    assert statuses.count("ok") == 2

    # OK markers exist
    ok_dir = tmp_path / "profiles" / "runs" / "testrun" / "ok"
    assert (ok_dir / "IE00AAA11111.ok").exists()
    assert (ok_dir / "IE00BBB22222.ok").exists()


def test_run_batch_skips_existing_when_not_force(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_profile_dict: dict[str, str]
) -> None:
    """If a profile exists and force=False, it should be skipped and not re-downloaded."""

    index_entries = [
        {"isin": "IE00AAA11111", "url": "http://dummy/etf1"},
        {"isin": "IE00BBB22222", "url": "http://dummy/etf2"},
    ]

    def fake_get_profile_index(*args, **kwargs):
        return index_entries

    def fake_download_html(isin: str, url: str, timeout: int = 30) -> str:
        return "<html>dummy</html>"

    def fake_parse_profile(html: str, isin: str):
        profile = fake_profile_dict.copy()
        profile["isin"] = isin
        return profile

    # Pre-create one profile JSON to simulate already cached
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    (profiles_dir / "IE00AAA11111.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.get_profile_index", fake_get_profile_index
    )
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.download_etf_profile_html",
        fake_download_html,
    )
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.parse_profile", fake_parse_profile
    )

    snapshot_path = run_batch(tmp_path, rate_seconds=0.0, force=False, run_id="skiprun")
    assert snapshot_path.exists()

    # Progress should contain one skip and one ok
    progress_lines = (
        (tmp_path / "profiles" / "runs" / "skiprun" / "progress.jsonl")
        .read_text(encoding="utf-8")
        .strip()
        .splitlines()
    )
    statuses = [json.loads(line)["status"] for line in progress_lines]
    assert statuses.count("skip") == 1
    assert statuses.count("ok") == 1


def test_run_batch_logs_error_and_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_profile_dict: dict[str, str]
) -> None:
    """If one download fails, it should log an error and continue processing others."""

    index_entries = [
        {"isin": "GOOD00000001", "url": "http://dummy/ok"},
        {"isin": "BAD000000002", "url": "http://dummy/fail"},
    ]

    def fake_get_profile_index(*args, **kwargs):
        return index_entries

    def fake_download_html(isin: str, url: str, timeout: int = 30) -> str:
        if "fail" in url:
            raise RuntimeError("network boom")
        return "<html>dummy</html>"

    def fake_parse_profile(html: str, isin: str):
        profile = fake_profile_dict.copy()
        profile["isin"] = isin
        return profile

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.get_profile_index", fake_get_profile_index
    )
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.download_etf_profile_html",
        fake_download_html,
    )
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.parse_profile", fake_parse_profile
    )

    snapshot_path = run_batch(tmp_path, rate_seconds=0.0, force=True, run_id="errrun")
    assert snapshot_path.exists()

    # One ok, one err
    progress_lines = (
        (tmp_path / "profiles" / "runs" / "errrun" / "progress.jsonl")
        .read_text(encoding="utf-8")
        .strip()
        .splitlines()
    )
    statuses = [json.loads(line)["status"] for line in progress_lines]
    assert statuses.count("ok") == 1
    assert statuses.count("err") == 1

    # Error marker exists
    err_dir = tmp_path / "profiles" / "runs" / "errrun" / "err"
    assert any(p.name.startswith("BAD000000002") for p in err_dir.glob("*.json"))


def test_run_batch_respects_run_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_profile_dict: dict[str, str]
) -> None:
    """Custom run_id should be used as the runs/ subdirectory name."""
    index_entries = [{"isin": "IE00AAA11111", "url": "http://dummy/etf1"}]

    def fake_get_profile_index(*args, **kwargs):
        return index_entries

    def fake_download_html(isin: str, url: str, timeout: int = 30) -> str:
        return "<html>dummy</html>"

    def fake_parse_profile(html: str, isin: str):
        profile = fake_profile_dict.copy()
        profile["isin"] = isin
        return profile

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.get_profile_index", fake_get_profile_index
    )
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.download_etf_profile_html",
        fake_download_html,
    )
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.parse_profile", fake_parse_profile
    )

    _ = run_batch(tmp_path, rate_seconds=0.0, force=True, run_id="myrunid-123")
    assert (tmp_path / "profiles" / "runs" / "myrunid-123").exists()
