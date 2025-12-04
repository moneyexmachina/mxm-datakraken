# mxm-datakraken

**mxm-datakraken** is the data collection package of the **Money Ex Machina** ecosystem.  
It provides connectors and scrapers to external data sources (APIs, broker exports, websites),  
and writes them into structured raw dumps for ingestion by other MXM packages.

---

## Purpose

- Collect raw data from external providers.  
- Normalize minimally into JSON/CSV.  
- Persist raw dumps to a local store for later import.  
- Keep messy, source-specific logic **outside** the canonical `mxm-refdata` package.

## Scope

Examples of connectors that live here:
- **ETF metadata** from [justETF](https://www.justetf.com/).  
- **Broker positions** (e.g. Hargreaves Lansdown exports).  
- **Exchange specifications** (e.g. CME contract metadata).  
- Other APIs and file dumps.

Out of scope:
- Ontology / canonical reference models (`mxm-refdata`).  
- Market prices and tick data (`mxm-marketdata`).  
- Analytics and strategy logic.

## Data Flow

```
External Source -> mxm-datakraken -> raw JSON/CSV
                -> mxm-refdata importer -> ORM models
                -> MXM ecosystem
```


## Project Structure

```
./src/mxm/datakraken/
    sources/
        justetf/      # ETF scraper
        hl/           # Broker positions connector
    utils/            # Shared scraping helpers
docs/
    feature.md        # Vision and scope
tests/
```

## Status

- **MVP Goal**: justETF scraper by ISIN → JSON dump.  
- Future: additional connectors, orchestration, scheduling.

## License

MIT (to confirm, same as other MXM packages).
