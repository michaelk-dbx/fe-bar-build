-- name: Latest training run (MLflow, logged by 03_train_fraud_model)
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
