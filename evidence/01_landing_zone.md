# Step 1: Synthetic source data landed in Unity Catalog

**What ran:** `uv run python src/data_gen/generate.py --out data/raw` (seed 42, as-of 2026-11-01), then `src/data_gen/land.sh mkr_claims` copied each source-system folder into the UC volume `landing` (prod target). The hidden ground truth went to the separate `simulation` volume, which the pipeline never reads.

Target: `serverless_stable_mkr_catalog.mkr_claims` · captured 2026-11-02 15:12 UTC on SQL warehouse `22d236430dbb5df0`

## Generator output
```
MKR & Co. synthetic data — seed=42, as of 2026-11-01
policyholders/policies : 50,000
contractors            : 450 (14 in fraud ring)
weather events         : 234
claims (FNOL)          : 7,340 across 47 monthly files
  fraud (ground truth) : 570 (7.8%)
  open (last 30 days)  : 188
  feed issues          : 73 resubmissions (fnol_version 2), 15 malformed records
  claims by peril      : {'water': 2358, 'wind': 1332, 'theft': 1328, 'hail': 1284, 'fire': 641, 'freeze': 397}
fraud patterns planted : {'inflated': 216, 'weather_mismatch': 209, 'inception': 153, 'narrative': 150, 'late_report': 131, 'contractor_ring': 111}
--- legacy SIU process (closed claims) ---
referred to SIU        : 597 (8.3% of closed)
SIU hit rate           : 18.4%
fraud caught           : 110 of 549
fraud leakage (paid)   : $10,343,340 on 439 claims
total paid             : $107,366,829
```

## Read-back of the raw FNOL files in the landing volume

Includes resubmissions and malformed records, which the pipeline handles in step 2.

```sql
SELECT substr(submitted_at, 1, 4) AS submitted_year, count(*) AS fnol_records,
       count_if(fnol_version = 2) AS resubmissions, count_if(claim_id IS NULL) AS missing_claim_id,
       round(sum(claimed_amount)) AS claimed_usd, count(DISTINCT _metadata.file_name) AS files
FROM read_files('/Volumes/serverless_stable_mkr_catalog/mkr_claims/landing/claims_fnol/', format => 'json')
GROUP BY 1 ORDER BY 1
```

Statement `01f1d6cf-c302-14b4-b60a-0376af892906` · 4 row(s)

| submitted_year | fnol_records | resubmissions | missing_claim_id | claimed_usd | files |
|---|---|---|---|---|---|
| 2023 | 1,118 | 9 | 1 | 20,545,021 | 12 |
| 2024 | 1,436 | 16 | 0 | 26,050,912 | 12 |
| 2025 | 2,304 | 21 | 3 | 40,291,008 | 12 |
| 2026 | 2,570 | 27 | 1 | 45,369,482 | 11 |

## Files per source system in the landing volume

```sql
SELECT split(_metadata.file_path, '/')[5] AS source_folder, count(DISTINCT _metadata.file_name) AS files
FROM read_files('/Volumes/serverless_stable_mkr_catalog/mkr_claims/landing/', format => 'text', recursiveFileLookup => true)
GROUP BY 1 ORDER BY 1
```

Statement `01f1d6cf-c470-1880-905c-a8e0a33917e7` · 5 row(s)

| source_folder | files |
|---|---|
| claims_fnol | 47 |
| policy_admin | 2 |
| siu | 1 |
| vendors | 1 |
| weather | 1 |
