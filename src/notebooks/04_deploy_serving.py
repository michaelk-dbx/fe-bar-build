# Databricks notebook source
# MAGIC %md
# MAGIC # Serve the champion fraud model for real-time scoring
# MAGIC The adjuster app scores new claims as they're entered (FNOL intake) through this endpoint.
# MAGIC Batch scoring of the claim queue (`05_score_claims`) loads the same UC model version directly.
# MAGIC Scale-to-zero keeps a demo endpoint cheap; production would keep a minimum of 1 for latency.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_mkr_catalog")
dbutils.widgets.text("schema", "mkr_claims")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")

from datetime import timedelta
import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

MODEL_NAME = f"{catalog}.{schema}.claims_fraud_model"
ENDPOINT = f"{schema.replace('_', '-')}-fraud"
mlflow.set_registry_uri("databricks-uc")
version = mlflow.MlflowClient().get_model_version_by_alias(MODEL_NAME, "champion").version
print(f"serving {MODEL_NAME} v{version} on endpoint {ENDPOINT}")

# COMMAND ----------

w = WorkspaceClient()
served = [ServedEntityInput(name="fraud", entity_name=MODEL_NAME, entity_version=str(version),
                            workload_size="Small", scale_to_zero_enabled=True)]
existing = {e.name for e in w.serving_endpoints.list()}
if ENDPOINT in existing:
    ep = w.serving_endpoints.update_config_and_wait(ENDPOINT, served_entities=served, timeout=timedelta(minutes=40))
else:
    ep = w.serving_endpoints.create_and_wait(ENDPOINT, config=EndpointCoreConfigInput(name=ENDPOINT, served_entities=served),
                                             timeout=timedelta(minutes=40))
print("state:", ep.state.ready, ep.state.config_update)

# COMMAND ----------

# Smoke test with two open claims from the gold table
sample = spark.sql(f"""
  SELECT claim_id, days_since_policy_start, days_since_coverage_increase, report_lag_days, claimed_amount,
         amount_to_coverage_ratio, amount_per_sqft, amount_vs_peril_median,
         CAST(storm_matched AS DOUBLE) AS storm_matched, matched_storm_severity, contractor_prior_claims,
         contractor_prior_confirmed_fraud, policy_prior_claims_3y, CAST(public_adjuster AS DOUBLE) AS public_adjuster,
         CAST(contractor_id IS NOT NULL AS DOUBLE) AS has_contractor, dwelling_coverage, deductible, square_feet,
         year_built, roof_age_years, peril, product, state
  FROM `{catalog}`.`{schema}`.gold_claim_features WHERE claim_status = 'open' ORDER BY claim_id LIMIT 2
""").toPandas()
records = sample.drop(columns="claim_id").astype(object).where(sample.drop(columns="claim_id").notna(), None).to_dict("records")
resp = w.serving_endpoints.query(ENDPOINT, dataframe_records=records)
for cid, pred in zip(sample.claim_id, resp.predictions):
    print(f"{cid}: fraud_score={pred[1]:.4f}")
