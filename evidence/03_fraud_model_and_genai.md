# Step 3: Fraud model (MLflow + UC + serving) and GenAI triage notes

**What ran:** the same prod job `mkr_claims_build` continued after the pipeline: `03_train_fraud_model` (scikit-learn gradient-boosted trees, MLflow, UC model `claims_fraud_model`, alias `champion`) → `04_deploy_serving` (endpoint `mkr-claims-fraud`) and `05_score_claims` (risk tiers, reason codes, `ai_query` triage notes). **Bottom line (2026 test claims, same SIU capacity of 178 referrals):** the model refers **106** real frauds vs **42** under the legacy rules (59.6% vs 23.6% precision), cutting fraud paid out by **$1.83M** in Jan–Oct 2026, and fast-tracks 45.5% of claims at a 1.2% fraud rate.

Target: `serverless_stable_mkr_catalog.mkr_claims` · captured 2026-11-04 08:34 UTC on SQL warehouse `22d236430dbb5df0`

## Job run: `mkr_claims_build` (prod)

Run ID `876998901967409` · started 2026-11-03 15:51:54 UTC · result **SUCCESS**

| task | result | duration (s) |
|---|---|---|
| governance_setup | SUCCESS | 27 |
| lakeflow_pipeline | SUCCESS | 73 |
| train_fraud_model | SUCCESS | 76 |
| deploy_serving | SUCCESS | 689 |
| score_claims | SUCCESS | 485 |
| governance_tags | SUCCESS | 48 |


## Real-time scoring: serving endpoint `mkr-claims-fraud` (UC model `claims_fraud_model@champion`)

`POST /serving-endpoints/mkr-claims-fraud/invocations` with two hand-built FNOL claims:

| test claim | key features | P(fraud) |
|---|---|---|
| Suspicious hail claim | hail, $38,000, 25 days after policy start, storm_matched=0.0, report lag 45d, contractor prior fraud=3, public adjuster=1.0 | 0.86865 |
| Routine water claim | water, $9,000, 900 days after policy start, storm_matched=None, report lag 2d, contractor prior fraud=0, public adjuster=0.0 | 0.00005 |


## Latest training run (MLflow, logged by 03_train_fraud_model)

Trained on SIU-confirmed labels for claims reported before 2026, tested on 2026. `sim_*` metrics use the hidden ground truth and are only possible because the data is synthetic.

```sql
SELECT m.key AS metric, m.value
FROM (SELECT trained_at, result FROM serverless_stable_mkr_catalog.mkr_claims.ml_training_runs ORDER BY trained_at DESC LIMIT 1)
LATERAL VIEW explode(from_json(result, 'map<string,string>')) m AS key, value
WHERE m.key NOT IN ('model', 'fast_track_max_amount')
ORDER BY metric
```

Statement `01f1d82a-9974-1316-9429-12001915521e` · 18 row(s)

| metric | value |
|---|---|
| fast_track | 0.0001326 |
| refer_siu | 0.006832 |
| run_id | af7784dfc3074aefb8dce31d60027b56 |
| sim_auc_vs_truth | 0.8969 |
| sim_fast_track_fraud_rate | 0.01214 |
| sim_fast_track_share | 0.4554 |
| sim_leakage_reduction_usd | 1,829,416 |
| sim_legacy_frauds_referred | 42 |
| sim_legacy_leakage_usd | 3,353,404 |
| sim_legacy_referral_precision | 0.236 |
| sim_model_frauds_referred | 106 |
| sim_model_leakage_usd | 1,523,987 |
| sim_model_referral_precision | 0.5955 |
| sim_true_fraud_in_test | 190 |
| siu_capacity_k | 178 |
| test_auc_siu_labels | 0.9024 |
| test_avg_precision_siu_labels | 0.1235 |
| version | 1 |

## Risk tiers for every claim (gold_claim_scores)

```sql
SELECT claim_status, risk_tier, count(*) AS claims, round(avg(risk_score)) AS avg_risk_score,
       round(sum(claimed_amount)) AS claimed_usd
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_scores GROUP BY ALL ORDER BY claim_status, avg_risk_score DESC
```

Statement `01f1d82a-9abb-16cb-8c7c-3d9e3aa7a3aa` · 6 row(s)

| claim_status | risk_tier | claims | avg_risk_score | claimed_usd |
|---|---|---|---|---|
| closed | refer_siu | 594 | 96 | 22,906,227 |
| closed | adjuster_review | 3,239 | 67 | 71,321,789 |
| closed | fast_track | 3,319 | 25 | 32,884,415 |
| open | refer_siu | 20 | 95 | 911,494 |
| open | adjuster_review | 81 | 69 | 2,235,191 |
| open | fast_track | 87 | 25 | 803,042 |

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

Statement `01f1d82a-9b8d-1878-a167-8aab7b7e170b` · 3 row(s)

| risk_tier | claims | true_fraud | fraud_rate | claimed_usd |
|---|---|---|---|---|
| refer_siu | 178 | 106 | 0.596 | 7,073,402 |
| adjuster_review | 1,103 | 71 | 0.064 | 23,350,156 |
| fast_track | 1,071 | 13 | 0.012 | 10,573,092 |

## Top of today's open-claim queue

```sql
SELECT claim_id, peril, round(claimed_amount) AS claimed_usd, risk_score, risk_tier, array_join(reason_codes, '; ') AS reason_codes
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_scores WHERE claim_status = 'open'
ORDER BY fraud_score DESC LIMIT 5
```

Statement `01f1d82a-9cf5-16cc-bf54-f43dcb594fc7` · 5 row(s)

| claim_id | peril | claimed_usd | risk_score | risk_tier | reason_codes |
|---|---|---|---|---|---|
| CLM0007324 | wind | 7,815 | 99 | refer_siu | No wind storm recorded in ZIP3 278 within 3 days of the loss; Reported 80 days after the loss; 3 prior claims on this policy in 3 years |
| CLM0007189 | hail | 85,935 | 99 | refer_siu | No hail storm recorded in ZIP3 278 within 3 days of the loss; Claimed amount is 5.7x the typical hail claim; Public adjuster involved; 2 prior claims on this policy in 3 years |
| CLM0007329 | wind | 26,662 | 98 | refer_siu | No wind storm recorded in ZIP3 283 within 3 days of the loss; Claimed amount is 3.0x the typical wind claim; Reported 87 days after the loss |
| CLM0007266 | hail | 24,367 | 98 | refer_siu | No hail storm recorded in ZIP3 274 within 3 days of the loss; Reported 85 days after the loss |
| CLM0007191 | wind | 103,502 | 98 | refer_siu | No wind storm recorded in ZIP3 654 within 3 days of the loss; Claimed amount is 11.5x the typical wind claim |

## GenAI triage notes (ai_query, structured output): coverage

```sql
SELECT llm_endpoint, count(*) AS open_claims, count(triage.summary) AS notes_generated,
       count_if(size(triage.narrative_red_flags) > 0) AS with_narrative_red_flags
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_ai_triage GROUP BY llm_endpoint
```

Statement `01f1d82a-9df4-1d6d-b696-d302eda05450` · 1 row(s)

| llm_endpoint | open_claims | notes_generated | with_narrative_red_flags |
|---|---|---|---|
| databricks-gemini-3-8-flash | 188 | 183 | 81 |

## GenAI triage note: highest-risk open claim

```sql
SELECT claim_id, risk_tier, risk_score, triage.summary, array_join(triage.narrative_red_flags, ' | ') AS red_flags, triage.recommended_next_step
FROM serverless_stable_mkr_catalog.mkr_claims.gold_claim_ai_triage ORDER BY risk_score DESC, claim_id LIMIT 1
```

Statement `01f1d82a-9eba-123f-ba44-515820768c19` · 1 row(s)

| claim_id | risk_tier | risk_score | summary | red_flags | recommended_next_step |
|---|---|---|---|---|---|
| CLM0007189 | refer_siu | 99 | Claim reports hail damage to roof and window screens, plus an interior bedroom leak, totaling $85,935. The automated fraud risk score is 99/100 (refer_siu). Key indicators include absence of meteorological verification for hail within the regional ZIP code around the reported timeframe, an indeterminate date of loss from the insured, unusually high claim severity, public adjuster involvement, and frequent prior claim history. | Insured is uncertain of the specific date of loss \| No recorded hail storm activity in ZIP3 278 within three days of the reported date of loss \| Claimed amount of $85,935 significantly exceeds baseline averages (5.7x typical hail claim severity) \| Representation by a public adjuster \| Elevated loss frequency with two prior claims filed on this policy within the past three years \| Reported interior leak in the primary bedroom requiring origin and cause verification alongside exterior hail impacts | Refer file to the Special Investigation Unit (SIU) in accordance with the tier refer_siu risk score, and assign an independent engineering firm to verify storm history, date of loss, and causation of both exterior impacts and interior leaks. |
