# justETF Source Notes

## Robots.txt (retrieved 2025-10-01)

```
User-agent: *
Allow: /
Disallow: /servlet/
Disallow: /link/
Disallow: /*/search.html*_wicket=1*
Sitemap: https://www.justetf.com/sitemap_index.xml
```

- ETF profile pages (e.g. `/en/etf-profile.html?isin=...`) are **allowed**.  
- Only dynamic search and internal servlet/link endpoints are disallowed.  

## Compliance Note

According to the **justETF General Terms and Conditions** (Section 3.1, retrieved 2025-10-01):

> *The user undertakes to refrain from anything that could impair the operability or one or more functionalities or the infrastructure of justETF. This includes, in particular, putting justETF under excessive strain as well as using programs to carry out automated price inquiries.*

**Interpretation for mxm-datakraken**:
- Our connector does **not** perform automated price inquiries.  
- It retrieves **ETF metadata only** (e.g. ISIN, name, provider, TER, replication method, distribution policy, listings).  
- To comply with the ToS, the connector:
  - Applies strict **rate limiting** (≥1–2s between requests).  
  - Identifies itself with a **custom User-Agent** (`mxm-datakraken/0.1`).  
  - Caches results locally to avoid repeated requests.  
- No high-frequency or bulk scraping of price data is performed.  
- This design ensures we do not impair operability or place excessive strain on justETF infrastructure.  

## Design Impact

- The justETF connector in `mxm-datakraken` will:  
  - Fetch ETF metadata by ISIN, on demand.  
  - Store results as raw JSON dumps in `data/raw/etf/`.  
  - Apply caching and polite rate limits by default.  
- Future extensions (sitemap-driven batch collection) must follow the same constraints.  
