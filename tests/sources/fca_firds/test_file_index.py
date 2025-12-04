import pytest
from mxm.types import JSONLike, JSONObj

import mxm_datakraken.sources.fca_firds.file_index as fi

# -----------------------------
# Unit tests with mocked API
# -----------------------------


class DummyResp:
    """Minimal mock of requests.Response for our tests."""

    def __init__(
        self,
        status_code: int = 200,
        json_data: JSONLike | None = None,
    ) -> None:
        self.status_code = status_code
        self._json = json_data or {}

    def json(self) -> JSONLike:
        return self._json

    def raise_for_status(self) -> None:
        if self.status_code != 200:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_discover_files_parses_hits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure discover_files returns FirdsFile list from sample JSON."""

    sample_json = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "download_link": "https://data.fca.org.uk/artefacts/FIRDS/FULINS_C_20250101_01of01.zip",
                        "file_type": "FULINS",
                        "file_name": "FULINS_C_20250101_01of01.zip",
                        "publication_date": "2025-01-01",
                    }
                }
            ]
        }
    }

    def fake_request(params: JSONObj, *args: object, **kwargs: object) -> JSONLike:
        _ = params, args, kwargs
        return sample_json

    monkeypatch.setattr(fi, "_request_with_backoff", fake_request)

    results = fi.discover_files("FULINS", "2025-01-01", "2025-01-01")
    assert len(results) == 1
    f = results[0]
    assert isinstance(f, fi.FirdsFile)
    assert f.file_type == "FULINS"
    assert f.file_name.startswith("FULINS_C_")
    assert f.publication_date == "2025-01-01"
    assert f.download_link.startswith("https://")


def test_discover_latest_publication_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure discover_latest_publication_date extracts latest date."""

    sample_json = {"hits": {"hits": [{"_source": {"publication_date": "2025-02-15"}}]}}

    def fake_request(params: JSONObj, *args: object, **kwargs: object) -> JSONLike:
        _ = params, args, kwargs
        return sample_json

    monkeypatch.setattr(fi, "_request_with_backoff", fake_request)

    result = fi.discover_latest_publication_date("FULINS")
    assert result == "2025-02-15"


def test_discover_latest_full_etf_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure discover_latest_full_etf_bucket calls discover_files with C bucket."""

    calls: dict[str, object] = {}

    def fake_latest_date(file_type: str) -> str:
        _ = file_type
        return "2025-03-01"

    def fake_discover_files(
        file_type: str,
        start_date: str,
        end_date: str,
        file_name_wildcard: str | None = None,
        **kwargs: object,
    ) -> list[fi.FirdsFile]:
        _ = kwargs
        calls.update(
            dict(
                file_type=file_type,
                start=start_date,
                end=end_date,
                wildcard=file_name_wildcard,
            )
        )
        return [
            fi.FirdsFile(
                "FULINS",
                "FULINS_C_20250301_01of01.zip",
                "2025-03-01",
                "https://example.com/file.zip",
            )
        ]

    monkeypatch.setattr(fi, "discover_latest_publication_date", fake_latest_date)
    monkeypatch.setattr(fi, "discover_files", fake_discover_files)

    results = fi.discover_latest_full_etf_bucket()
    assert len(results) == 1
    assert calls["file_type"] == "FULINS"
    assert calls["wildcard"] == "FULINS_C_*"


# -----------------------------
# Integration test (optional)
# -----------------------------


@pytest.mark.integration
def test_integration_latest_date_and_files() -> None:
    """Hit the live FCA FIRDS API (slow, network)."""
    latest_date = fi.discover_latest_publication_date("FULINS")
    assert latest_date is not None
    files = fi.discover_files(
        "FULINS", latest_date, latest_date, file_name_wildcard="FULINS_C_*"
    )
    assert isinstance(files, list)
    # Usually at least one 'C' file is present
    assert all(isinstance(f, fi.FirdsFile) for f in files)
