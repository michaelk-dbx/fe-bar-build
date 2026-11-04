# Databricks notebook source
# MAGIC %md
# MAGIC # Score every claim and prepare the adjuster triage queue
# MAGIC 1. **Fraud score** from the champion UC model, plus a 0-100 `risk_score` (percentile) for display.
# MAGIC 2. **Risk tier** from the thresholds stored on the model version: `refer_siu` (same SIU capacity as today),
# MAGIC    `fast_track` (low score and claim ≤ $25k), else `adjuster_review`.
# MAGIC 3. **Reason codes**: plain-English statements of which known fraud indicators fire on the claim. These are
# MAGIC    deterministic and auditable; the model decides *how much* they matter.
# MAGIC 4. **GenAI triage note** for open claims: an LLM (`ai_query`) reads the adjuster narrative plus the reason codes and
# MAGIC    writes a short summary, narrative red flags and a next step for the adjuster. Structured output (`responseFormat`)
# MAGIC    guarantees the schema; Claude endpoints reject it (it needs forced tool use), so a Gemini Flash endpoint is used.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_mkr_catalog")
dbutils.widgets.text("schema", "mkr_claims")
dbutils.widgets.text("llm_endpoint", "databricks-gemini-3-8-flash")
catalog, schema, llm = (dbutils.widgets.get(k) for k in ("catalog", "schema", "llm_endpoint"))
target = f"`{catalog}`.`{schema}`"

import json
import mlflow
import pandas as pd

MODEL_NAME = f"{catalog}.{schema}.claims_fraud_model"
mlflow.set_registry_uri("databricks-uc")
mv = mlflow.MlflowClient().get_model_version_by_alias(MODEL_NAME, "champion")
thresholds = json.loads(mv.tags["risk_thresholds"])
model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@champion")
FEATURES = model.feature_names_in_.tolist()
print(f"model v{mv.version} | thresholds {thresholds}")

# COMMAND ----------

df = spark.table(f"{target}.gold_claim_features").toPandas()
df["has_contractor"] = df.contractor_id.notna()
for c in ["storm_matched", "public_adjuster", "has_contractor"]:
    df[c] = df[c].map({True: 1.0, False: 0.0})
num = [c for c in FEATURES if c not in ("peril", "product", "state")]
df[num] = df[num].astype(float)
scores = pd.DataFrame({"claim_id": df.claim_id, "fraud_score": model.predict_proba(df[FEATURES])[:, 1]})
spark.createDataFrame(scores).createOrReplaceTempView("model_scores")

spark.sql(f"""
CREATE OR REPLACE TABLE {target}.gold_claim_scores
COMMENT 'Fraud score, risk tier and reason codes for every claim, from the champion claims_fraud_model'
AS SELECT
  f.claim_id, f.claim_status, f.report_date, f.state, f.peril, f.claimed_amount, f.contractor_id,
  round(s.fraud_score, 4) AS fraud_score,
  -- Display score for adjusters: the model ranks well (AUC ~0.9) but isn't calibrated, so show the claim's percentile
  CAST(round(100 * percent_rank() OVER (ORDER BY s.fraud_score)) AS INT) AS risk_score,
  CASE WHEN s.fraud_score >= {thresholds['refer_siu']} THEN 'refer_siu'
       WHEN s.fraud_score < {thresholds['fast_track']} AND f.claimed_amount <= {thresholds['fast_track_max_amount']} THEN 'fast_track'
       ELSE 'adjuster_review' END AS risk_tier,
  filter(array(
    CASE WHEN f.storm_matched = false THEN concat('No ', f.peril, ' storm recorded in ZIP3 ', f.zip3, ' within 3 days of the loss') END,
    CASE WHEN f.days_since_policy_start < 60 THEN concat('Loss ', f.days_since_policy_start, ' days after the policy started') END,
    CASE WHEN f.days_since_coverage_increase BETWEEN 0 AND 59 THEN concat('Loss ', f.days_since_coverage_increase, ' days after a coverage increase') END,
    CASE WHEN f.amount_vs_peril_median >= 2 THEN concat('Claimed amount is ', round(f.amount_vs_peril_median, 1), 'x the typical ', f.peril, ' claim') END,
    CASE WHEN f.contractor_prior_confirmed_fraud > 0 THEN concat('Contractor ', f.contractor_id, ' has ', f.contractor_prior_confirmed_fraud, ' prior SIU-confirmed fraud claim(s)') END,
    CASE WHEN f.report_lag_days >= 30 THEN concat('Reported ', f.report_lag_days, ' days after the loss') END,
    CASE WHEN f.public_adjuster THEN 'Public adjuster involved' END,
    CASE WHEN f.policy_prior_claims_3y >= 2 THEN concat(f.policy_prior_claims_3y, ' prior claims on this policy in 3 years') END
  ), x -> x IS NOT NULL) AS reason_codes,
  {mv.version} AS model_version,
  current_timestamp() AS scored_at
FROM {target}.gold_claim_features f JOIN model_scores s USING (claim_id)
""")
display(spark.sql(f"SELECT claim_status, risk_tier, count(*) AS claims, round(avg(fraud_score), 3) AS avg_score FROM {target}.gold_claim_scores GROUP BY ALL ORDER BY ALL"))

# COMMAND ----------

# GenAI triage notes for the open-claim queue (the claims adjusters work today)
spark.sql(f"""
CREATE OR REPLACE TABLE {target}.gold_claim_ai_triage
COMMENT 'LLM triage note per open claim: summary, narrative red flags and recommended next step (ai_query, {llm})'
AS WITH open_claims AS (
  SELECT s.claim_id, s.peril, s.claimed_amount, s.fraud_score, s.risk_score, s.risk_tier, s.reason_codes, f.description
  FROM {target}.gold_claim_scores s JOIN {target}.gold_claim_features f USING (claim_id)
  WHERE s.claim_status = 'open'
)
SELECT claim_id, risk_tier, risk_score,
  from_json(ai_query('{llm}',
    concat('You are a senior property claims adjuster at a US homeowners insurer. Write a triage note for this claim. ',
           'Be factual and neutral: flag indicators, never accuse the policyholder. ',
           'Peril: ', peril, '. Claimed amount: $', format_number(claimed_amount, 0), '. ',
           'Fraud risk score: ', risk_score, '/100, i.e. riskier than ', risk_score, '% of claims (tier ', risk_tier, '). ',
           'Indicators from claim data: ', coalesce(nullif(array_join(reason_codes, '; '), ''), 'none'), '. ',
           'Adjuster narrative: ', description),
    responseFormat => 'STRUCT<triage:STRUCT<summary:STRING, narrative_red_flags:ARRAY<STRING>, recommended_next_step:STRING>>'),
    'STRUCT<summary:STRING, narrative_red_flags:ARRAY<STRING>, recommended_next_step:STRING>') AS triage,
  '{llm}' AS llm_endpoint, current_timestamp() AS generated_at
FROM open_claims
""")
display(spark.sql(f"SELECT claim_id, risk_tier, risk_score, triage.* FROM {target}.gold_claim_ai_triage ORDER BY risk_score DESC LIMIT 5"))
