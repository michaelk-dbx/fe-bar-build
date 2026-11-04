# Step 3: Fraud model (MLflow + UC + serving) and GenAI triage notes

**What ran:** after EDA and tuning ([03a](03a_feature_eda_and_tuning.md)), `05_train_fraud_model` (scikit-learn gradient-boosted trees, MLflow, UC model `claims_fraud_model` v4, alias `champion`) → `06_deploy_serving` (endpoint `mkr-claims-fraud`) and `07_score_claims` (risk tiers, reason codes, `ai_query` triage notes). **Bottom line (2026 test claims, same SIU capacity of 178 referrals):** the model refers **131** real frauds vs **42** under the legacy rules (73.6% vs 23.6% precision), cutting fraud paid out by **$1.37M** in Jan–Oct 2026, and fast-tracks 45.5% of claims with 1 fraud in 1,071. Run ID `265878132716662` of prod job `mkr_claims_build` · result **SUCCESS** (governance → pipeline → feature_eda → hyperparam_search → train → serve → score → tags).

Target: `serverless_stable_mkr_catalog.mkr_claims` · captured 2026-11-04 15:11 UTC on SQL warehouse `22d236430dbb5df0`

## Latest training run (MLflow, logged by 05_train_fraud_model)

Trained on SIU-confirmed labels for claims reported before 2026, tested on 2026. `sim_*` metrics use the hidden ground truth and are only possible because the data is synthetic.

```sql
SELECT m.key AS metric, m.value
FROM (SELECT trained_at, result FROM serverless_stable_mkr_catalog.mkr_claims.ml_training_runs ORDER BY trained_at DESC LIMIT 1)
LATERAL VIEW explode(from_json(result, 'map<string,string>')) m AS key, value
WHERE m.key NOT IN ('model', 'fast_track_max_amount')
ORDER BY metric
```

Statement `01f1d862-0553-1b18-8ed3-4a38ed33c48f` · 18 row(s)

| metric | value |
|---|---|
| fast_track | 0.001899 |
| refer_siu | 0.01965 |
| run_id | 1145a64c82824b278bf98b3e0d01aaaa |
| sim_auc_vs_truth | 0.956 |
| sim_fast_track_fraud_rate | 0.0009337 |
| sim_fast_track_share | 0.4554 |
| sim_leakage_reduction_usd | 1,370,923 |
| sim_legacy_frauds_referred | 42 |
| sim_legacy_leakage_usd | 3,353,404 |
| sim_legacy_referral_precision | 0.236 |
| sim_model_frauds_referred | 131 |
| sim_model_leakage_usd | 1,982,480 |
| sim_model_referral_precision | 0.736 |
| sim_true_fraud_in_test | 190 |
| siu_capacity_k | 178 |
| test_auc_siu_labels | 0.9232 |
| test_avg_precision_siu_labels | 0.1933 |
| version | 4 |

## Risk tiers for every claim (gold_claim_scores)

```sql
SELECT claim_status, risk_tier, count(*) AS claims, round(avg(risk_score)) AS avg_risk_score,
       round(sum(claimed_amount)) AS claimed_usd
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_scores GROUP BY ALL ORDER BY claim_status, avg_risk_score DESC
```

Statement `01f1d862-0628-184b-a051-e88d148f9f98` · 6 row(s)

| claim_status | risk_tier | claims | avg_risk_score | claimed_usd |
|---|---|---|---|---|
| closed | refer_siu | 623 | 96 | 23,527,973 |
| closed | adjuster_review | 3,798 | 64 | 76,012,885 |
| closed | fast_track | 2,731 | 19 | 27,571,573 |
| open | refer_siu | 19 | 96 | 782,006 |
| open | adjuster_review | 117 | 66 | 2,698,960 |
| open | fast_track | 52 | 26 | 468,761 |

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

Statement `01f1d862-0709-19e2-8d28-16db5992ff8f` · 3 row(s)

| risk_tier | claims | true_fraud | fraud_rate | claimed_usd |
|---|---|---|---|---|
| refer_siu | 178 | 131 | 0.736 | 6,743,640 |
| adjuster_review | 1,103 | 58 | 0.053 | 23,528,858 |
| fast_track | 1,071 | 1 | 0.001 | 10,724,152 |

## Top of today's open-claim queue

```sql
SELECT claim_id, peril, round(claimed_amount) AS claimed_usd, risk_score, risk_tier, array_join(reason_codes, '; ') AS reason_codes
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_scores WHERE claim_status = 'open'
ORDER BY fraud_score DESC LIMIT 5
```

Statement `01f1d862-08a8-1827-b639-916bb0491c4b` · 5 row(s)

| claim_id | peril | claimed_usd | risk_score | risk_tier | reason_codes |
|---|---|---|---|---|---|
| CLM0007191 | wind | 103,502 | 99 | refer_siu | No wind storm recorded in ZIP3 654 within 3 days of the loss; Claimed amount is 11.5x the typical wind claim |
| CLM0007189 | hail | 85,935 | 99 | refer_siu | No hail storm recorded in ZIP3 278 within 3 days of the loss; Claimed amount is 5.7x the typical hail claim; Public adjuster involved; 2 prior claims on this policy in 3 years |
| CLM0007266 | hail | 24,367 | 97 | refer_siu | No hail storm recorded in ZIP3 274 within 3 days of the loss; Reported 85 days after the loss |
| CLM0007184 | hail | 7,180 | 97 | refer_siu | No hail storm recorded in ZIP3 271 within 3 days of the loss |
| CLM0007329 | wind | 26,662 | 97 | refer_siu | No wind storm recorded in ZIP3 283 within 3 days of the loss; Claimed amount is 3.0x the typical wind claim; Reported 87 days after the loss |

## GenAI triage notes (ai_query, structured output): coverage

```sql
SELECT llm_endpoint, count(*) AS open_claims, count(triage.summary) AS notes_generated,
       count_if(size(triage.narrative_red_flags) > 0) AS with_narrative_red_flags
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_ai_triage GROUP BY llm_endpoint
```

Statement `01f1d862-0979-13e2-8811-ca298ec24bc3` · 1 row(s)

| llm_endpoint | open_claims | notes_generated | with_narrative_red_flags |
|---|---|---|---|
| databricks-gemini-3-8-flash | 188 | 186 | 102 |

## GenAI triage note: highest-risk open claim

```sql
SELECT claim_id, risk_tier, risk_score, triage.summary, array_join(triage.narrative_red_flags, ' | ') AS red_flags, triage.recommended_next_step
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_ai_triage ORDER BY risk_score DESC, claim_id LIMIT 1
```

Statement `01f1d862-0a43-1a01-a280-31da97c386fa` · 1 row(s)

| claim_id | risk_tier | risk_score | summary | red_flags | recommended_next_step |
|---|---|---|---|---|---|
| CLM0007189 | refer_siu | 99 | Triage review for hail loss totaling $85,935 with an overall fraud risk score of 99/100 (refer_siu tier). The claim involves reported roof, window screen, and interior leak damage with an unconfirmed loss date, no regional weather verification within the reported timeframe, elevated claim severity, public adjuster representation, and multiple recent prior losses. | Uncertainty from the insured regarding the exact date of loss \| Absence of recorded hail events in ZIP3 278 within a 3-day window of the reported loss date \| Claimed amount of $85,935 significantly exceeds typical hail severity benchmark (5.7x higher) \| Involvement of a public adjuster \| Prior loss frequency with 2 claims filed in the preceding 3 years | Refer file to Special Investigation Unit (SIU) pursuant to risk score threshold, pull historical weather forensic reporting for the risk address, and conduct an independent engineering inspection to verify age, cause, and origin of the documented roof and interior damage. |
