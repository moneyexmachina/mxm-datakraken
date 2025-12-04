# Changelog

All notable changes to this project will be documented in this file.

The format is based on **[Keep a Changelog](https://keepachangelog.com/en/1.1.0/)**,  
and this project adheres to **[Semantic Versioning](https://semver.org/spec/v2.0.0.html)**.

---

## [Unreleased]

### Planned
- CLI command to fetch or inspect any source dynamically (`mxm-datakraken cli`).
- Support for eternal sources such as FCA FIRDS with frozen `as_of_bucket`s.
- Integration tests covering mixed volatility sources.
- Optional JSON schema validation for ingested payloads.

---

## [0.3.0] – 2025-10-28
### Added
- Adopted **`mxm-dataio 0.3.0`** with policy-driven caching (`CacheMode`, `ttl_seconds`, `as_of_bucket`, `cache_tag`).
- Implemented **volatile daily snapshot model** for *justETF* data source:
  - New `as_of_bucket = YYYY-MM-DD` for each collection day.
  - Historical buckets retained indefinitely for diffing and audit.
- Introduced `sources.justetf` section in `config/default.yaml` with cache policy fields.
- Extended `inspect_snapshot.py` to list available buckets and compare snapshots.

### Changed
- justETF adapter now initialises `DataIoSession` with explicit cache policy.
- Dependency bump: `mxm-dataio → ^0.3.0`.

---

## [0.2.0] – 2025-10-10
### Added
- Initial working prototype of **mxm-datakraken**:
  - justETF adapter for ETF profile index and profile downloads.
  - Config views for dataio, http, and paths.
  - Inspection CLI for snapshot browsing.
- Complete integration with `mxm-config` and `mxm-dataio` ingestion layer.
- Full test suite and Ruff/Pyright compliance.

---

