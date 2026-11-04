# Step 3: Fraud model (MLflow + UC + serving) and GenAI triage notes

**What ran:** after EDA and tuning ([03a](03a_feature_eda_and_tuning.md)), `05_train_fraud_model` (scikit-learn gradient-boosted trees, MLflow, UC model `claims_fraud_model` v3, alias `champion`) → `06_deploy_serving` (endpoint `mkr-claims-fraud`) and `07_score_claims` (risk tiers, reason codes, `ai_query` triage notes). **Bottom line (2026 test claims, same SIU capacity of 178 referrals):** the model refers **109** real frauds vs **42** under the legacy rules (61.2% vs 23.6% precision), cutting fraud paid out by **$1.42M** in Jan–Oct 2026, and fast-tracks 44.8% of claims at a 0.8% fraud rate. Run ID `151534464161572` of prod job `mkr_claims_build` · result **SUCCESS** (governance → pipeline → feature_eda → hyperparam_search → train → serve → score → tags).

Target: `serverless_stable_mkr_catalog.mkr_claims` · captured 2026-11-04 11:52 UTC on SQL warehouse `22d236430dbb5df0`

## Latest training run (MLflow, logged by 05_train_fraud_model)

Trained on SIU-confirmed labels for claims reported before 2026, tested on 2026. `sim_*` metrics use the hidden ground truth and are only possible because the data is synthetic.

```sql
SELECT m.key AS metric, m.value
FROM (SELECT trained_at, result FROM serverless_stable_mkr_catalog.mkr_claims.ml_training_runs ORDER BY trained_at DESC LIMIT 1)
LATERAL VIEW explode(from_json(result, 'map<string,string>')) m AS key, value
WHERE m.key NOT IN ('model', 'fast_track_max_amount')
ORDER BY metric
```

Statement `01f1d846-30b6-163a-ac6c-cee0d80ab264` · 18 row(s)

| metric | value |
|---|---|
| fast_track | 4.773e-06 |
| refer_siu | 0.0003792 |
| run_id | 58ef0c37a8834b5486e5249500c3d24d |
| sim_auc_vs_truth | 0.9075 |
| sim_fast_track_fraud_rate | 0.00759 |
| sim_fast_track_share | 0.4481 |
| sim_leakage_reduction_usd | 1,420,940 |
| sim_legacy_frauds_referred | 42 |
| sim_legacy_leakage_usd | 3,353,404 |
| sim_legacy_referral_precision | 0.236 |
| sim_model_frauds_referred | 109 |
| sim_model_leakage_usd | 1,932,464 |
| sim_model_referral_precision | 0.6124 |
| sim_true_fraud_in_test | 190 |
| siu_capacity_k | 178 |
| test_auc_siu_labels | 0.8989 |
| test_avg_precision_siu_labels | 0.1558 |
| version | 3 |

## Risk tiers for every claim (gold_claim_scores)

```sql
SELECT claim_status, risk_tier, count(*) AS claims, round(avg(risk_score)) AS avg_risk_score,
       round(sum(claimed_amount)) AS claimed_usd
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_scores GROUP BY ALL ORDER BY claim_status, avg_risk_score DESC
```

Statement `01f1d846-317c-183c-a970-239cd7d324c2` · 6 row(s)

| claim_status | risk_tier | claims | avg_risk_score | claimed_usd |
|---|---|---|---|---|
| closed | refer_siu | 587 | 96 | 25,599,886 |
| closed | adjuster_review | 3,242 | 67 | 65,119,558 |
| closed | fast_track | 3,323 | 25 | 36,392,987 |
| open | refer_siu | 18 | 96 | 845,735 |
| open | adjuster_review | 94 | 66 | 2,339,777 |
| open | fast_track | 76 | 31 | 764,214 |

## Simulation check: tiers vs ground truth on 2026 closed claims (test period)

refer_siu concentrates real fraud; fast_track is almost fraud-free.

```sql
WITH truth AS (SELECT claim_id, lower(CAST(is_fraud AS STRING)) = 'true' AS is_fraud
               FROM read_files('/Volumes/serverless_stable_mkr_catalog/mkr_claims/simulation/fraud_truth.csv', format => 'csv', header => true))
SELECT s.risk_tier, count(*) AS claims, count_if(t.is_fraud) AS true_fraud,
       round(count_if(t.is_fraud) / count(*), 3) AS fraud_rate, round(sum(s.claimed_amount)) AS claimed_usd
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_scores s JOIN truth t USING (claim_id)
WHERE s.claim_status = 'closed' AND s.report_date >= '2026-01-01'
GROUP BY ALL ORDER BY fraud_rate DESC
```

Statement `01f1d846-3263-1fe0-8885-41cf70bcff61` · 3 row(s)

| risk_tier | claims | true_fraud | fraud_rate | claimed_usd |
|---|---|---|---|---|
| refer_siu | 178 | 109 | 0.612 | 7,795,243 |
| adjuster_review | 1,120 | 73 | 0.065 | 21,679,059 |
| fast_track | 1,054 | 8 | 0.008 | 11,522,348 |

## Top of today's open-claim queue

```sql
SELECT claim_id, peril, round(claimed_amount) AS claimed_usd, risk_score, risk_tier, array_join(reason_codes, '; ') AS reason_codes
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_scores WHERE claim_status = 'open'
ORDER BY fraud_score DESC LIMIT 5
```

Statement `01f1d846-33bf-1937-8e73-bfe68a105bd6` · 5 row(s)

| claim_id | peril | claimed_usd | risk_score | risk_tier | reason_codes |
|---|---|---|---|---|---|
| CLM0007191 | wind | 103,502 | 99 | refer_siu | No wind storm recorded in ZIP3 654 within 3 days of the loss; Claimed amount is 11.5x the typical wind claim |
| CLM0007189 | hail | 85,935 | 99 | refer_siu | No hail storm recorded in ZIP3 278 within 3 days of the loss; Claimed amount is 5.7x the typical hail claim; Public adjuster involved; 2 prior claims on this policy in 3 years |
| CLM0007324 | wind | 7,815 | 99 | refer_siu | No wind storm recorded in ZIP3 278 within 3 days of the loss; Reported 80 days after the loss; 3 prior claims on this policy in 3 years |
| CLM0007184 | hail | 7,180 | 99 | refer_siu | No hail storm recorded in ZIP3 271 within 3 days of the loss |
| CLM0007329 | wind | 26,662 | 98 | refer_siu | No wind storm recorded in ZIP3 283 within 3 days of the loss; Claimed amount is 3.0x the typical wind claim; Reported 87 days after the loss |

## GenAI triage notes (ai_query, structured output): coverage

```sql
SELECT llm_endpoint, count(*) AS open_claims, count(triage.summary) AS notes_generated,
       count_if(size(triage.narrative_red_flags) > 0) AS with_narrative_red_flags
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_ai_triage GROUP BY llm_endpoint
```

Statement `01f1d846-347b-12dc-a011-a7a8d5638a90` · 1 row(s)

| llm_endpoint | open_claims | notes_generated | with_narrative_red_flags |
|---|---|---|---|
| databricks-gemini-3-8-flash | 188 | 183 | 86 |

## GenAI triage note: highest-risk open claim

```sql
SELECT claim_id, risk_tier, risk_score, triage.summary, array_join(triage.narrative_red_flags, ' | ') AS red_flags, triage.recommended_next_step
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_ai_triage ORDER BY risk_score DESC, claim_id LIMIT 1
```

Statement `01f1d846-355e-1b2c-83bc-4fb9ebc237a8` · 1 row(s)

| claim_id | risk_tier | risk_score | summary | red_flags | recommended_next_step |
|---|---|---|---|---|---|
| CLM0007184 | refer_siu | 99 | Claim reports $7,180 in hail damage to roof shingles, gutters, and siding. Third-party weather verification indicates no hail activity in ZIP3 271 within 3 days of the loss date. The policyholder has requested an immediate cash settlement, and the automated risk score is 99/100. | No recorded hail storm in the geographic area (ZIP3 271) within 3 days of the reported date of loss \| Insured is specifically requesting an expedited cash settlement \| High fraud risk model score of 99/100, placing the file in the refer_siu tier | Refer file to the Special Investigation Unit (SIU) for further review and verify meteorological data for the reported loss location and date prior to issuing any payment. |
