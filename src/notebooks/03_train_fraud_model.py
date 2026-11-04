# Databricks notebook source
# MAGIC %md
# MAGIC # Train the claims fraud model
# MAGIC * **Labels:** what MKR & Co. actually knows, i.e. SIU-confirmed fraud on closed claims (`label_confirmed_fraud`).
# MAGIC   Undetected fraud is labelled 0, as it would be in real life.
# MAGIC * **Split by time:** train on claims reported before 2026, test on 2026, the way the model would be used.
# MAGIC * **Model:** gradient-boosted trees (scikit-learn `HistGradientBoostingClassifier`). It handles missing values
# MAGIC   (e.g. no coverage increase, no storm check for water claims) and mixed numeric/categorical features natively.
# MAGIC * **Registry:** Unity Catalog model `claims_fraud_model`, alias `champion`.
# MAGIC * **Simulation-only evaluation:** at the end, the hidden ground truth measures how much fraud the model would catch
# MAGIC   versus the legacy rules at the same SIU capacity. A real insurer can't measure that.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_mkr_catalog")
dbutils.widgets.text("schema", "mkr_claims")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")

import json
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

NUMERIC = ["days_since_policy_start", "days_since_coverage_increase", "report_lag_days", "claimed_amount",
           "amount_to_coverage_ratio", "amount_per_sqft", "amount_vs_peril_median", "storm_matched",
           "matched_storm_severity", "contractor_prior_claims", "contractor_prior_confirmed_fraud",
           "policy_prior_claims_3y", "public_adjuster", "has_contractor", "dwelling_coverage", "deductible",
           "square_feet", "year_built", "roof_age_years"]
CATEGORICAL = ["peril", "product", "state"]
FEATURES = NUMERIC + CATEGORICAL
MODEL_NAME = f"{catalog}.{schema}.claims_fraud_model"


def load_features():
    df = spark.table(f"`{catalog}`.`{schema}`.gold_claim_features").toPandas()
    df["has_contractor"] = df.contractor_id.notna()
    for c in ["storm_matched", "public_adjuster", "has_contractor"]:
        df[c] = df[c].map({True: 1.0, False: 0.0})
    df[NUMERIC] = df[NUMERIC].astype(float)
    df["report_date"] = pd.to_datetime(df.report_date)
    return df


df = load_features()
closed = df[df.claim_status == "closed"]
train, test = closed[closed.report_date < "2026-01-01"], closed[closed.report_date >= "2026-01-01"]
print(f"train {len(train):,} claims ({int(train.label_confirmed_fraud.sum())} confirmed fraud) | "
      f"test {len(test):,} claims ({int(test.label_confirmed_fraud.sum())} confirmed fraud)")

# COMMAND ----------

params = dict(max_iter=300, learning_rate=0.05, max_leaf_nodes=15, min_samples_leaf=20,
              l2_regularization=1.0, class_weight="balanced", random_state=0)
model = Pipeline([
    ("encode", ColumnTransformer(
        [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CATEGORICAL)],
        remainder="passthrough", verbose_feature_names_out=False)),
    ("gbt", HistGradientBoostingClassifier(categorical_features=[0, 1, 2], **params)),
])

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/mkr_claims_fraud_{schema}")

with mlflow.start_run(run_name="hgb_siu_labels") as run:
    model.fit(train[FEATURES], train.label_confirmed_fraud.astype(int))
    p_test = model.predict_proba(test[FEATURES])[:, 1]

    # Real-world metrics: against the labels MKR & Co. has (SIU outcomes)
    k = int(test.siu_referred.sum())  # same SIU capacity as the legacy process
    top_k = test.assign(p=p_test).nlargest(k, "p")
    metrics = {
        "test_auc_siu_labels": roc_auc_score(test.label_confirmed_fraud, p_test),
        "test_avg_precision_siu_labels": average_precision_score(test.label_confirmed_fraud, p_test),
        "siu_capacity_k": k,
    }
    # Risk tiers: refer the same share of claims SIU handles today; fast-track the lower half if the amount is modest
    thresholds = {"refer_siu": float(np.quantile(p_test, 1 - k / len(test))),
                  "fast_track": float(np.quantile(p_test, 0.5)), "fast_track_max_amount": 25000.0}

    signature = infer_signature(test[FEATURES].head(50), model.predict_proba(test[FEATURES].head(50)))
    mlflow.log_params({**params, "features": ",".join(FEATURES), "train_cutoff": "2026-01-01"})
    mlflow.log_metrics(metrics)
    mlflow.log_dict(thresholds, "risk_thresholds.json")
    info = mlflow.sklearn.log_model(model, name="model", signature=signature, input_example=test[FEATURES].head(3),
                                    pyfunc_predict_fn="predict_proba", registered_model_name=MODEL_NAME)

    # Simulation-only: how much fraud is actually caught? (ground truth never touches training)
    truth = pd.read_csv(f"/Volumes/{catalog}/{schema}/simulation/fraud_truth.csv")[["claim_id", "is_fraud"]]
    ev = test.assign(p=p_test).merge(truth, on="claim_id")
    legacy, modeled = ev[ev.siu_referred], ev.nlargest(k, "p")
    fast = ev[(ev.p < thresholds["fast_track"]) & (ev.claimed_amount <= thresholds["fast_track_max_amount"])]
    sim = {
        "sim_true_fraud_in_test": int(ev.is_fraud.sum()),
        "sim_auc_vs_truth": roc_auc_score(ev.is_fraud, ev.p),
        "sim_legacy_referral_precision": legacy.is_fraud.mean(), "sim_legacy_frauds_referred": int(legacy.is_fraud.sum()),
        "sim_model_referral_precision": modeled.is_fraud.mean(), "sim_model_frauds_referred": int(modeled.is_fraud.sum()),
        "sim_fast_track_share": len(fast) / len(ev), "sim_fast_track_fraud_rate": fast.is_fraud.mean(),
        # Fraud paid out because it was never referred: legacy vs model referral list (85% SIU confirmation rate)
        "sim_legacy_leakage_usd": ev[ev.is_fraud & (ev.siu_outcome != "confirmed_fraud")].paid_amount.sum(),
        "sim_model_leakage_usd": ev[ev.is_fraud].paid_amount.sum()
                                 - 0.85 * ev[ev.is_fraud & ev.claim_id.isin(modeled.claim_id)].paid_amount.sum(),
    }
    sim["sim_leakage_reduction_usd"] = sim["sim_legacy_leakage_usd"] - sim["sim_model_leakage_usd"]
    mlflow.log_metrics(sim)

version = info.registered_model_version
client = mlflow.MlflowClient()
client.set_registered_model_alias(MODEL_NAME, "champion", version)
client.set_model_version_tag(MODEL_NAME, version, "risk_thresholds", json.dumps(thresholds))

# COMMAND ----------

result = {"model": MODEL_NAME, "version": int(version), "run_id": run.info.run_id, **metrics, **sim, **thresholds}
print(json.dumps({k: (round(v, 4) if isinstance(v, float) else v) for k, v in result.items()}, indent=2))
spark.createDataFrame([(json.dumps(result, default=float),)], "result STRING") \
    .selectExpr("current_timestamp() AS trained_at", "result") \
    .write.mode("append").saveAsTable(f"`{catalog}`.`{schema}`.ml_training_runs")
dbutils.jobs.taskValues.set("model_version", int(version))
