# Step 3a: Feature EDA and distributed hyperparameter search

**What ran:** `03_feature_eda` checks every candidate feature on the training period only (leakage, missing rate, single-feature AUC, Spearman redundancy, permutation importance on 2025) and writes a keep/drop/exclude decision with its reason to `ml_feature_analysis`. `04_hyperparam_search` runs 96 random-search trials × 3 rolling time windows in parallel on serverless Spark (`mapInPandas`), logs them to MLflow and `ml_tuning_trials`, and hands the best configuration to training. The feature set is itself a search dimension, so the EDA's choice is tested, not assumed. **Result:** 5 features dropped, 4 excluded as leakage; the winning trial uses the EDA-selected set; 288 model fits in 95 s wall-clock (5.9x parallel speedup). Run ID `151534464161572` of prod job `mkr_claims_build` · result **SUCCESS** (governance → pipeline → feature_eda → hyperparam_search → train → serve → score → tags).

Target: `serverless_stable_mkr_catalog.mkr_claims` · captured 2026-11-04 11:52 UTC on SQL warehouse `22d236430dbb5df0`

## Feature EDA decisions (ml_feature_analysis, training period only)

AUC = single-feature separation of SIU-confirmed fraud; perm = drop in average precision on 2025 when the feature is shuffled (model fit on 2023-24).

```sql
SELECT feature, decision, round(univariate_auc, 3) AS auc, round(perm_importance, 3) AS perm, round(perm_importance_std, 3) AS perm_std,
       round(missing_rate, 2) AS missing, reason
FROM serverless_stable_mkr_catalog.mkr_claims.ml_feature_analysis
ORDER BY CASE decision WHEN 'keep' THEN 0 WHEN 'drop' THEN 1 ELSE 2 END, perm_importance DESC
```

Statement `01f1d846-2e39-171a-82b9-5a739805a85f` · 26 row(s)

| feature | decision | auc | perm | perm_std | missing | reason |
|---|---|---|---|---|---|---|
| days_since_policy_start | keep | 0.804 | 0.209 | 0.016 | 0 | carries signal |
| amount_vs_peril_median | keep | 0.715 | 0.071 | 0.02 | 0 | carries signal |
| amount_per_sqft | keep | 0.711 | 0.066 | 0.018 | 0 | carries signal |
| claimed_amount | keep | 0.713 | 0.056 | 0.02 | 0 | carries signal |
| state | keep | 0.609 | 0.051 | 0.02 | 0 | carries signal |
| report_lag_days | keep | 0.667 | 0.035 | 0.026 | 0 | carries signal |
| storm_matched | keep | 0.72 | 0.034 | 0.037 | 0.63 | carries signal |
| matched_storm_severity | keep | 0.511 | 0.029 | 0.024 | 0.68 | carries signal |
| square_feet | keep | 0.531 | 0.017 | 0.015 | 0 | carries signal |
| roof_age_years | keep | 0.553 | 0.016 | 0.012 | 0 | carries signal |
| has_contractor | keep | 0.568 | 0.012 | 0.007 | 0 | carries signal |
| deductible | keep | 0.52 | 0.003 | 0.003 | 0 | carries signal |
| policy_prior_claims_3y | keep | 0.568 | 0.002 | 0.002 | 0 | carries signal |
| public_adjuster | keep | 0.62 | 0.001 | 0.002 | 0 | carries signal |
| contractor_prior_confirmed_fraud | keep | 0.55 | 0 | 0 | 0 | weak in this data but kept as a known indicator of contractor fraud rings |
| days_since_coverage_increase | keep | 0.507 | 0 | 0.001 | 0.93 | weak in this data but kept as a known indicator of inception fraud after a coverage increase |
| peril | keep | 0.614 | -0.014 | 0.014 | 0 | carries signal |
| dwelling_coverage | drop | 0.506 | 0.012 | 0.025 | 0 | no univariate signal and no measurable value to the model |
| contractor_prior_claims | drop | 0.533 | 0.005 | 0.009 | 0 | no univariate signal and no measurable value to the model |
| year_built | drop | 0.512 | 0.003 | 0.006 | 0 | no univariate signal and no measurable value to the model |
| product | drop | 0.523 | -0.002 | 0.003 | 0 | no univariate signal and no measurable value to the model |
| amount_to_coverage_ratio | drop | 0.702 | -0.005 | 0.015 | 0 | redundant with amount_per_sqft (rho 0.98), which the model relies on more |
| siu_referred | exclude | NULL | NULL | NULL | NULL | target leakage: known only after the claim closes |
| siu_outcome | exclude | NULL | NULL | NULL | NULL | target leakage: known only after the claim closes |
| paid_amount | exclude | NULL | NULL | NULL | NULL | target leakage: known only after the claim closes |
| claim_status | exclude | NULL | NULL | NULL | NULL | target leakage: known only after the claim closes |

## Distributed hyperparameter search: scale

Trials run in parallel on serverless Spark workers (mapInPandas); speedup = total compute time / wall-clock time.

```sql
SELECT count(*) AS trials, count(*) * 3 AS model_fits, round(sum(compute_seconds)) AS compute_seconds,
       round(max(search_wall_seconds)) AS wall_seconds, round(sum(compute_seconds) / max(search_wall_seconds), 1) AS parallel_speedup
FROM serverless_stable_mkr_catalog.mkr_claims.ml_tuning_trials
WHERE search_id = (SELECT max_by(search_id, searched_at) FROM serverless_stable_mkr_catalog.mkr_claims.ml_tuning_trials)
```

Statement `01f1d846-2f18-141e-863c-91ef37504fa8` · 1 row(s)

| trials | model_fits | compute_seconds | wall_seconds | parallel_speedup |
|---|---|---|---|---|
| 96 | 288 | 559 | 95 | 5.9 |

## Top trials vs the default configuration (trial 0)

Mean average precision over 3 time windows (2024 H2, 2025 H1, 2025 H2); std shows how noisy ~70 labels are.

```sql
SELECT rank, trial_id, round(mean_ap, 3) AS mean_ap, round(std_ap, 3) AS std_ap, round(mean_auc, 3) AS mean_auc,
       get_json_object(params, '$.feature_set') AS features, get_json_object(params, '$.class_weight') AS class_weight,
       round(CAST(get_json_object(params, '$.learning_rate') AS DOUBLE), 3) AS lr, get_json_object(params, '$.max_leaf_nodes') AS leaves,
       get_json_object(params, '$.min_samples_leaf') AS min_leaf, get_json_object(params, '$.l2_regularization') AS l2
FROM serverless_stable_mkr_catalog.mkr_claims.ml_tuning_trials
WHERE search_id = (SELECT max_by(search_id, searched_at) FROM serverless_stable_mkr_catalog.mkr_claims.ml_tuning_trials) AND (rank <= 5 OR trial_id = 0)
ORDER BY rank
```

Statement `01f1d846-2fe2-1169-9f40-56d955d7bf99` · 6 row(s)

| rank | trial_id | mean_ap | std_ap | mean_auc | features | class_weight | lr | leaves | min_leaf | l2 |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 75 | 0.326 | 0.092 | 0.933 | selected | none | 0.254 | 63 | 10 | 5 |
| 2 | 55 | 0.312 | 0.165 | 0.894 | all | none | 0.207 | 63 | 20 | 0 |
| 3 | 63 | 0.311 | 0.135 | 0.905 | all | none | 0.19 | 15 | 20 | 0.1 |
| 4 | 9 | 0.311 | 0.115 | 0.936 | selected | none | 0.021 | 7 | 100 | 1 |
| 5 | 10 | 0.305 | 0.087 | 0.922 | selected | none | 0.054 | 7 | 50 | 5 |
| 59 | 0 | 0.264 | 0.105 | 0.908 | all | balanced | 0.05 | 15 | 20 | 1 |
