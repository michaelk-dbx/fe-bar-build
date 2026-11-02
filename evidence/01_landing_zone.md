# Step 1 — Synthetic source data landed in Unity Catalog

**What ran:** `uv run python src/data_gen/generate.py --out data/raw` (seed 42, as-of 2026-11-01), then `databricks fs cp` of each source-system folder into the UC volume `serverless_stable_mkr_catalog.mkr_claims.landing`. Ground truth went to the separate `simulation` volume, which the pipeline never reads.

## Generator output
```
MKR & Co. synthetic data — seed=42, as of 2026-11-01
policyholders/policies : 50,000
contractors            : 450 (14 in fraud ring)
weather events         : 234
claims (FNOL)          : 7,340 across 47 monthly files
  fraud (ground truth) : 570 (7.8%)
  open (last 30 days)  : 188
  claims by peril      : {'water': 2358, 'wind': 1332, 'theft': 1328, 'hail': 1284, 'fire': 641, 'freeze': 397}
fraud patterns planted : {'inflated': 216, 'weather_mismatch': 209, 'inception': 153, 'narrative': 150, 'late_report': 131, 'contractor_ring': 111}
--- legacy SIU process (closed claims) ---
referred to SIU        : 597 (8.3% of closed)
SIU hit rate           : 18.4%
fraud caught           : 110 of 549
fraud leakage (paid)   : $10,343,340 on 439 claims
total paid             : $107,366,829
```

## Files in the landing volume (`databricks fs ls`)
| Folder | Files |
|---|---|
| policy_admin | 2 (policyholders.csv, policies.csv) |
| vendors | 1 (contractors.csv) |
| weather | 1 (weather_events.csv) |
| claims_fnol | 47 (claims_2023-01.jsonl … claims_2026-11.jsonl) |
| siu | 1 (claim_outcomes.csv) |
| simulation volume | fraud_truth.csv |

## Read-back check on the SQL warehouse
```sql
SELECT substr(report_date,1,4) AS report_year, count(*) AS claims,
       round(sum(claimed_amount)) AS claimed_usd, count(DISTINCT _metadata.file_name) AS files
FROM read_files('/Volumes/serverless_stable_mkr_catalog/mkr_claims/landing/claims_fnol/', format => 'json')
GROUP BY 1 ORDER BY 1
```
Statement `01f1d6c1-f71a-107d-bd37-76094dc49ea3` — SUCCEEDED

| report_year | claims | claimed_usd | files |
|---|---|---|---|
| 2023 | 1105 | 20,379,493 | 12 |
| 2024 | 1418 | 25,818,130 | 12 |
| 2025 | 2277 | 39,918,158 | 12 |
| 2026 | 2540 | 44,946,376 | 11 |

Total = 7,340 claims, matching the generator. Volume grows each year because MKR & Co.'s book grows.
