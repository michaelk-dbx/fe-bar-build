# Step 2a: Lakeflow pipeline: ingestion, data quality, features

**What ran:** `databricks bundle deploy -t prod` then `databricks bundle run -t prod mkr_claims_build`: mask functions → Lakeflow Spark Declarative Pipeline (`src/pipelines/*.sql`, serverless) → classification tags. Results were queried from the prod tables after that run. Governance evidence is in [02b](02b_unity_catalog_governance.md).

Target: `serverless_stable_mkr_catalog.mkr_claims` · captured 2026-11-02 15:13 UTC on SQL warehouse `22d236430dbb5df0`

## Job run: `mkr_claims_build` (prod)

Run ID `485353566926937` · started 2026-11-02 15:07:39 UTC · result **SUCCESS**

| task | result | duration (s) |
|---|---|---|
| governance_setup | SUCCESS | 27 |
| lakeflow_pipeline | SUCCESS | 93 |
| governance_tags | SUCCESS | 61 |


## Row counts through the medallion layers

7,428 FNOL submissions land in bronze; 15 malformed records are dropped by expectations; 73 resubmissions collapse into existing claims, leaving 7,340 unique claims.

```sql
SELECT 'bronze_claims_fnol' AS table_name, count(*) AS rows FROM serverless_stable_mkr_catalog.mkr_claims.bronze_claims_fnol
UNION ALL SELECT 'silver_claims_validated', count(*) FROM serverless_stable_mkr_catalog.mkr_claims.silver_claims_validated
UNION ALL SELECT 'silver_claims', count(*) FROM serverless_stable_mkr_catalog.mkr_claims.silver_claims
UNION ALL SELECT 'gold_claim_features', count(*) FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_features
UNION ALL SELECT 'gold_contractor_risk', count(*) FROM serverless_stable_mkr_catalog.mkr_claims.gold_contractor_risk
UNION ALL SELECT 'gold_claims_kpis_monthly', count(*) FROM serverless_stable_mkr_catalog.mkr_claims.gold_claims_kpis_monthly
```

Statement `01f1d6cf-e9ea-1cda-a8dd-c9362b814a34` · 6 row(s)

| table_name | rows |
|---|---|
| silver_claims | 7,340 |
| bronze_claims_fnol | 7,428 |
| silver_claims_validated | 7,413 |
| gold_claim_features | 7,340 |
| gold_contractor_risk | 450 |
| gold_claims_kpis_monthly | 1,618 |

## Data quality expectations (pipeline event log, latest update per dataset)

Read from the Lakeflow event log. Dropped rows are counted, not silently lost.

```sql
WITH dq AS (
  SELECT origin.update_id, timestamp,
         explode(from_json(details:flow_progress:data_quality:expectations,
           'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM event_log(TABLE(serverless_stable_mkr_catalog.mkr_claims.silver_claims_validated))
  WHERE event_type = 'flow_progress' AND details:flow_progress:data_quality:expectations IS NOT NULL
)
SELECT element_at(split(e.dataset, '[.]'), -1) AS dataset, e.name AS expectation,
       max_by(e.passed_records, timestamp) AS passed_records,
       max_by(e.failed_records, timestamp) AS failed_records
FROM dq
WHERE e.passed_records + e.failed_records > 0
GROUP BY ALL ORDER BY dataset, expectation
```

Statement `01f1d6cf-eba2-16b4-95b9-d93764875fc7` · 8 row(s)

| dataset | expectation | passed_records | failed_records |
|---|---|---|---|
| gold_claim_features | policy_found | 7,340 | 0 |
| silver_claim_outcomes | outcome_requires_referral | 7,152 | 0 |
| silver_claims_validated | has_claim_id | 7,423 | 5 |
| silver_claims_validated | has_policy_id | 7,428 | 0 |
| silver_claims_validated | known_peril | 7,428 | 0 |
| silver_claims_validated | positive_amount | 7,423 | 5 |
| silver_claims_validated | reported_after_loss | 7,423 | 5 |
| silver_policies | valid_coverage | 50,000 | 0 |

## Resubmitted claims are deduplicated by AUTO CDC

Bronze holds both versions; silver keeps one row per claim with the latest version (the updated description).

```sql
SELECT
  (SELECT count(*) FROM serverless_stable_mkr_catalog.mkr_claims.bronze_claims_fnol WHERE fnol_version = 2) AS resubmissions_in_bronze,
  (SELECT count(*) - count(DISTINCT claim_id) FROM serverless_stable_mkr_catalog.mkr_claims.silver_claims) AS duplicate_claim_ids_in_silver,
  (SELECT count(*) FROM serverless_stable_mkr_catalog.mkr_claims.silver_claims WHERE fnol_version = 2) AS claims_on_latest_version_2,
  (SELECT count(*) FROM serverless_stable_mkr_catalog.mkr_claims.silver_claims WHERE fnol_version = 2 AND description LIKE '%Update: photos%') AS with_updated_description
```

Statement `01f1d6cf-ec8f-103c-ba57-f1445aa5fb0c` · 1 row(s)

| resubmissions_in_bronze | duplicate_claim_ids_in_silver | claims_on_latest_version_2 | with_updated_description |
|---|---|---|---|
| 73 | 0 | 73 | 73 |

## Fraud signal in the gold features (closed claims, by SIU label)

The point-in-time features separate confirmed fraud from other closed claims; this is what the model in step 3 learns from.

```sql
SELECT label_confirmed_fraud,
       count(*) AS claims,
       round(avg(CASE WHEN days_since_policy_start < 60 OR days_since_coverage_increase < 60 THEN 1 ELSE 0 END), 3) AS share_loss_within_60d_of_start_or_increase,
       round(avg(CASE WHEN is_weather_peril THEN CAST(NOT storm_matched AS INT) END), 3) AS weather_claims_without_storm,
       round(avg(CASE WHEN contractor_prior_confirmed_fraud > 0 THEN 1 ELSE 0 END), 3) AS contractor_with_prior_fraud,
       round(avg(amount_vs_peril_median), 2) AS avg_amount_vs_peril_median,
       round(avg(report_lag_days), 1) AS avg_report_lag_days,
       round(avg(CAST(public_adjuster AS INT)), 3) AS public_adjuster_rate
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_features
WHERE claim_status = 'closed'
GROUP BY label_confirmed_fraud ORDER BY label_confirmed_fraud
```

Statement `01f1d6cf-ed74-1fe0-8a32-da8d128bf98c` · 2 row(s)

| label_confirmed_fraud | claims | share_loss_within_60d_of_start_or_increase | weather_claims_without_storm | contractor_with_prior_fraud | avg_amount_vs_peril_median | avg_report_lag_days | public_adjuster_rate |
|---|---|---|---|---|---|---|---|
| 0 | 7,042 | 0.092 | 0.094 | 0.036 | 1.29 | 6.6 | 0.098 |
| 1 | 110 | 0.536 | 0.965 | 0.136 | 2.78 | 22.2 | 0.345 |

## Contractor scorecard: highest confirmed-fraud rates (10+ claims)

```sql
SELECT contractor_id, contractor_name, state, claims, avg_amount_vs_peril_median, storm_mismatch_rate, siu_referrals, confirmed_fraud_claims, confirmed_fraud_rate
FROM serverless_stable_mkr_catalog.mkr_claims.gold_contractor_risk
WHERE claims >= 10 ORDER BY confirmed_fraud_rate DESC, claims DESC LIMIT 10
```

Statement `01f1d6cf-ee60-132e-b0e1-0e188e7fd6de` · 10 row(s)

| contractor_id | contractor_name | state | claims | avg_amount_vs_peril_median | storm_mismatch_rate | siu_referrals | confirmed_fraud_claims | confirmed_fraud_rate |
|---|---|---|---|---|---|---|---|---|
| CTR0098 | Hardin Roofing | TX | 12 | 2.946 | 1 | 5 | 5 | 0.417 |
| CTR0254 | Lewis Roofing | GA | 11 | 3.882 | 0.875 | 4 | 3 | 0.273 |
| CTR0063 | Frazier Restoration | TX | 11 | 1.958 | 0.2 | 3 | 2 | 0.182 |
| CTR0183 | Diaz Roofing | OK | 19 | 3.492 | 1 | 4 | 3 | 0.167 |
| CTR0450 | Bishop Home Repair | MO | 13 | 1.384 | 0.25 | 3 | 2 | 0.154 |
| CTR0163 | Sims Home Repair | FL | 10 | 1.374 | 1 | 2 | 1 | 0.111 |
| CTR0132 | Bradshaw Roofing | FL | 11 | 1.604 | NULL | 3 | 1 | 0.1 |
| CTR0055 | Adams Restoration | TX | 10 | 1.254 | 0.167 | 1 | 1 | 0.1 |
| CTR0442 | Alvarado Builders | MO | 10 | 1.19 | 0.25 | 2 | 1 | 0.1 |
| CTR0215 | Bush Restoration | CO | 10 | 1.718 | 0 | 1 | 1 | 0.1 |

## Legacy SIU performance by year (gold KPIs)

```sql
SELECT year(report_month) AS report_year, sum(claims) AS claims, round(sum(paid_usd)) AS paid_usd,
       sum(siu_referrals) AS siu_referrals, sum(confirmed_fraud_claims) AS confirmed_fraud,
       round(sum(confirmed_fraud_claims) / sum(siu_referrals), 3) AS siu_hit_rate, sum(open_claims) AS open_claims
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claims_kpis_monthly GROUP BY ALL ORDER BY report_year
```

Statement `01f1d6cf-ef38-1450-8b39-1329fe9de890` · 4 row(s)

| report_year | claims | paid_usd | siu_referrals | confirmed_fraud | siu_hit_rate | open_claims |
|---|---|---|---|---|---|---|
| 2023 | 1,105 | 17,393,787 | 95 | 17 | 0.179 | 0 |
| 2024 | 1,418 | 21,793,871 | 133 | 23 | 0.173 | 0 |
| 2025 | 2,277 | 33,799,225 | 191 | 33 | 0.173 | 0 |
| 2026 | 2,540 | 34,379,947 | 178 | 37 | 0.208 | 188 |
