-- name: Row counts through the medallion layers
-- note: 7,428 FNOL submissions land in bronze; 15 malformed records are dropped by expectations; 73 resubmissions collapse into existing claims, leaving 7,340 unique claims.
SELECT 'bronze_claims_fnol' AS table_name, count(*) AS rows FROM {catalog}.{schema}.bronze_claims_fnol
UNION ALL SELECT 'silver_claims_validated', count(*) FROM {catalog}.{schema}.silver_claims_validated
UNION ALL SELECT 'silver_claims', count(*) FROM {catalog}.{schema}.silver_claims
UNION ALL SELECT 'gold_claim_features', count(*) FROM {catalog}.{schema}.gold_claim_features
UNION ALL SELECT 'gold_contractor_risk', count(*) FROM {catalog}.{schema}.gold_contractor_risk
UNION ALL SELECT 'gold_claims_kpis_monthly', count(*) FROM {catalog}.{schema}.gold_claims_kpis_monthly;

-- name: Data quality expectations (pipeline event log, latest update per dataset)
-- note: Read from the Lakeflow event log. Dropped rows are counted, not silently lost.
WITH dq AS (
  SELECT origin.update_id, timestamp,
         explode(from_json(details:flow_progress:data_quality:expectations,
           'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM event_log(TABLE({catalog}.{schema}.silver_claims_validated))
  WHERE event_type = 'flow_progress' AND details:flow_progress:data_quality:expectations IS NOT NULL
)
SELECT element_at(split(e.dataset, '[.]'), -1) AS dataset, e.name AS expectation,
       max_by(e.passed_records, timestamp) AS passed_records,
       max_by(e.failed_records, timestamp) AS failed_records
FROM dq
WHERE e.passed_records + e.failed_records > 0
GROUP BY ALL ORDER BY dataset, expectation;

-- name: Resubmitted claims are deduplicated by AUTO CDC
-- note: Bronze holds both versions; silver keeps one row per claim with the latest version (the updated description).
SELECT
  (SELECT count(*) FROM {catalog}.{schema}.bronze_claims_fnol WHERE fnol_version = 2) AS resubmissions_in_bronze,
  (SELECT count(*) - count(DISTINCT claim_id) FROM {catalog}.{schema}.silver_claims) AS duplicate_claim_ids_in_silver,
  (SELECT count(*) FROM {catalog}.{schema}.silver_claims WHERE fnol_version = 2) AS claims_on_latest_version_2,
  (SELECT count(*) FROM {catalog}.{schema}.silver_claims WHERE fnol_version = 2 AND description LIKE '%Update: photos%') AS with_updated_description;

-- name: Fraud signal in the gold features (closed claims, by SIU label)
-- note: The point-in-time features separate confirmed fraud from other closed claims; this is what the model in step 3 learns from.
SELECT label_confirmed_fraud,
       count(*) AS claims,
       round(avg(CASE WHEN days_since_policy_start < 60 OR days_since_coverage_increase < 60 THEN 1 ELSE 0 END), 3) AS share_loss_within_60d_of_start_or_increase,
       round(avg(CASE WHEN is_weather_peril THEN CAST(NOT storm_matched AS INT) END), 3) AS weather_claims_without_storm,
       round(avg(CASE WHEN contractor_prior_confirmed_fraud > 0 THEN 1 ELSE 0 END), 3) AS contractor_with_prior_fraud,
       round(avg(amount_vs_peril_median), 2) AS avg_amount_vs_peril_median,
       round(avg(report_lag_days), 1) AS avg_report_lag_days,
       round(avg(CAST(public_adjuster AS INT)), 3) AS public_adjuster_rate
FROM {catalog}.{schema}.gold_claim_features
WHERE claim_status = 'closed'
GROUP BY label_confirmed_fraud ORDER BY label_confirmed_fraud;

-- name: Contractor scorecard: highest confirmed-fraud rates (10+ claims)
SELECT contractor_id, contractor_name, state, claims, avg_amount_vs_peril_median, storm_mismatch_rate, siu_referrals, confirmed_fraud_claims, confirmed_fraud_rate
FROM {catalog}.{schema}.gold_contractor_risk
WHERE claims >= 10 ORDER BY confirmed_fraud_rate DESC, claims DESC LIMIT 10;

-- name: Legacy SIU performance by year (gold KPIs)
SELECT year(report_month) AS report_year, sum(claims) AS claims, round(sum(paid_usd)) AS paid_usd,
       sum(siu_referrals) AS siu_referrals, sum(confirmed_fraud_claims) AS confirmed_fraud,
       round(sum(confirmed_fraud_claims) / sum(siu_referrals), 3) AS siu_hit_rate, sum(open_claims) AS open_claims
FROM {catalog}.{schema}.gold_claims_kpis_monthly GROUP BY ALL ORDER BY report_year;
