-- step: 01
-- name: Read-back of the raw FNOL files in the landing volume
-- note: Includes resubmissions and malformed records, which the pipeline handles in step 2.
SELECT substr(submitted_at, 1, 4) AS submitted_year, count(*) AS fnol_records,
       count_if(fnol_version = 2) AS resubmissions, count_if(claim_id IS NULL) AS missing_claim_id,
       round(sum(claimed_amount)) AS claimed_usd, count(DISTINCT _metadata.file_name) AS files
FROM read_files('/Volumes/{catalog}/{schema}/landing/claims_fnol/', format => 'json')
GROUP BY 1 ORDER BY 1;

-- name: Files per source system in the landing volume
SELECT split(_metadata.file_path, '/')[5] AS source_folder, count(DISTINCT _metadata.file_name) AS files
FROM read_files('/Volumes/{catalog}/{schema}/landing/', format => 'text', recursiveFileLookup => true)
GROUP BY 1 ORDER BY 1;

-- step: 02
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

-- step: 02b
-- name: PII column masks in action (query run by a user outside the SIU group)
SELECT is_account_group_member('mkr_siu') AS caller_in_siu_group, policyholder_id, full_name, email, phone, street_address, state, birth_year
FROM {catalog}.{schema}.silver_policyholders ORDER BY policyholder_id LIMIT 3;

-- name: Column masks and classification tags in Unity Catalog
SELECT 'column_mask' AS kind, table_name, column_name, mask_name AS value
FROM {catalog}.information_schema.column_masks WHERE table_schema = '{schema}'
UNION ALL
SELECT 'column_tag', table_name, column_name, tag_name
FROM {catalog}.information_schema.column_tags WHERE schema_name = '{schema}'
ORDER BY kind, table_name, column_name;

-- name: Table tags in Unity Catalog
SELECT table_name, concat_ws(', ', collect_list(concat(tag_name, '=', tag_value))) AS tags
FROM {catalog}.information_schema.table_tags WHERE schema_name = '{schema}'
GROUP BY table_name ORDER BY table_name;

-- step: 03a
-- name: Feature EDA decisions (ml_feature_analysis, training period only)
-- note: AUC = single-feature separation of SIU-confirmed fraud; perm = drop in average precision on 2025 when the feature is shuffled (model fit on 2023-24).
SELECT feature, decision, round(univariate_auc, 3) AS auc, round(perm_importance, 3) AS perm, round(perm_importance_std, 3) AS perm_std,
       round(missing_rate, 2) AS missing, reason
FROM {catalog}.{schema}.ml_feature_analysis
ORDER BY CASE decision WHEN 'keep' THEN 0 WHEN 'drop' THEN 1 ELSE 2 END, perm_importance DESC;

-- name: Distributed hyperparameter search: scale (Optuna TPE, batches on Spark)
-- note: Each batch of trials is fitted in parallel on serverless Spark workers (mapInPandas), then told back to the TPE sampler before the next batch. fit_seconds counts model fitting only.
SELECT count(*) AS trials, count(DISTINCT batch) AS batches, count(*) * 3 AS model_fits, round(sum(fit_seconds)) AS fit_seconds,
       round(max(search_wall_seconds)) AS wall_seconds, round(sum(fit_seconds) / max(search_wall_seconds), 1) AS fit_time_per_wall_second
FROM {catalog}.{schema}.ml_tuning_trials
WHERE search_id = (SELECT max_by(search_id, searched_at) FROM {catalog}.{schema}.ml_tuning_trials);

-- name: Best validation AP per batch (the sampler learning)
SELECT batch + 1 AS batch, round(max(mean_ap), 3) AS best_ap_in_batch, round(avg(mean_ap), 3) AS avg_ap_in_batch
FROM {catalog}.{schema}.ml_tuning_trials
WHERE search_id = (SELECT max_by(search_id, searched_at) FROM {catalog}.{schema}.ml_tuning_trials)
GROUP BY batch ORDER BY batch;

-- name: Top trials vs the current defaults (trial 0)
-- note: Mean average precision over 3 time windows (2024 H2, 2025 H1, 2025 H2); std shows how noisy ~70 labels are.
SELECT rank, trial_id, batch + 1 AS batch, round(mean_ap, 3) AS mean_ap, round(std_ap, 3) AS std_ap, round(mean_auc, 3) AS mean_auc,
       get_json_object(params, '$.feature_set') AS features, get_json_object(params, '$.class_weight') AS class_weight,
       round(CAST(get_json_object(params, '$.learning_rate') AS DOUBLE), 3) AS lr, get_json_object(params, '$.max_leaf_nodes') AS leaves,
       get_json_object(params, '$.min_samples_leaf') AS min_leaf, get_json_object(params, '$.l2_regularization') AS l2
FROM {catalog}.{schema}.ml_tuning_trials
WHERE search_id = (SELECT max_by(search_id, searched_at) FROM {catalog}.{schema}.ml_tuning_trials) AND (rank <= 5 OR trial_id = 0)
ORDER BY rank;

-- step: 03
-- name: Latest training run (MLflow, logged by 05_train_fraud_model)
-- note: Trained on SIU-confirmed labels for claims reported before 2026, tested on 2026. `sim_*` metrics use the hidden ground truth and are only possible because the data is synthetic.
SELECT m.key AS metric, m.value
FROM (SELECT trained_at, result FROM {catalog}.{schema}.ml_training_runs ORDER BY trained_at DESC LIMIT 1)
LATERAL VIEW explode(from_json(result, 'map<string,string>')) m AS key, value
WHERE m.key NOT IN ('model', 'fast_track_max_amount')
ORDER BY metric;

-- name: Risk tiers for every claim (gold_claim_scores)
SELECT claim_status, risk_tier, count(*) AS claims, round(avg(risk_score)) AS avg_risk_score,
       round(sum(claimed_amount)) AS claimed_usd
FROM {catalog}.{schema}.gold_claim_scores GROUP BY ALL ORDER BY claim_status, avg_risk_score DESC;

-- name: Simulation check: tiers vs ground truth on 2026 closed claims (test period)
-- note: refer_siu concentrates real fraud; fast_track is almost fraud-free.
WITH truth AS (SELECT claim_id, lower(CAST(is_fraud AS STRING)) = 'true' AS is_fraud
               FROM read_files('/Volumes/{catalog}/{schema}/simulation/fraud_truth.csv', format => 'csv', header => true))
SELECT s.risk_tier, count(*) AS claims, count_if(t.is_fraud) AS true_fraud,
       round(count_if(t.is_fraud) / count(*), 3) AS fraud_rate, round(sum(s.claimed_amount)) AS claimed_usd
FROM {catalog}.{schema}.gold_claim_scores s JOIN truth t USING (claim_id)
WHERE s.claim_status = 'closed' AND s.report_date >= '2026-01-01'
GROUP BY ALL ORDER BY fraud_rate DESC;

-- name: Top of today's open-claim queue
SELECT claim_id, peril, round(claimed_amount) AS claimed_usd, risk_score, risk_tier, array_join(reason_codes, '; ') AS reason_codes
FROM {catalog}.{schema}.gold_claim_scores WHERE claim_status = 'open'
ORDER BY fraud_score DESC LIMIT 5;

-- name: GenAI triage notes (ai_query, structured output): coverage
SELECT llm_endpoint, count(*) AS open_claims, count(triage.summary) AS notes_generated,
       count_if(size(triage.narrative_red_flags) > 0) AS with_narrative_red_flags
FROM {catalog}.{schema}.gold_claim_ai_triage GROUP BY llm_endpoint;

-- name: GenAI triage note: highest-risk open claim
SELECT claim_id, risk_tier, risk_score, triage.summary, array_join(triage.narrative_red_flags, ' | ') AS red_flags, triage.recommended_next_step
FROM {catalog}.{schema}.gold_claim_ai_triage ORDER BY risk_score DESC, claim_id LIMIT 1;

-- step: 04
-- name: Lakebase checks from the build job (lakebase_checks, latest run)
-- note: Row parity of the synced tables, adjuster-queue latency measured against Postgres, and the decision round trip through Lakebase CDF.
SELECT check, value, detail, passed FROM {catalog}.{schema}.lakebase_checks
WHERE checked_at = (SELECT max(checked_at) FROM {catalog}.{schema}.lakebase_checks) ORDER BY check;

-- name: The queue read back from Lakebase through Unity Catalog (synced table)
-- note: claim_scores_synced is the Postgres table, exposed in UC; open claims by tier as the app sees them.
SELECT risk_tier, count(*) AS open_claims, round(avg(risk_score)) AS avg_risk_score, round(sum(claimed_amount)) AS claimed_usd
FROM {catalog}.{schema}.claim_scores_synced WHERE claim_status = 'open' GROUP BY ALL ORDER BY avg_risk_score DESC;

-- name: Decision write-back: Postgres change history landed in UC Delta (Lakebase CDF)
SELECT _pg_change_type, _timestamp, claim_id, decision, adjuster, model_risk_tier, decided_at
FROM {catalog}.{schema}.lb_claim_decisions_history ORDER BY _pg_lsn DESC LIMIT 6;

-- name: Latest decision per claim (gold_adjuster_decisions, for Genie, SIU reporting and future labels)
SELECT claim_id, decision, adjuster, model_risk_tier, model_risk_score, decided_at, is_smoke_test
FROM {catalog}.{schema}.gold_adjuster_decisions ORDER BY decided_at DESC LIMIT 5;
