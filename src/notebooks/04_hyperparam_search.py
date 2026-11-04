# Databricks notebook source
# MAGIC %md
# MAGIC # Distributed hyperparameter search (Optuna TPE, batches on Spark)
# MAGIC * The driver's TPE study asks for `batch_size` trials; Spark fits them in parallel on serverless workers
# MAGIC   (`mapInPandas`); results are told back before the next batch. `constant_liar` keeps a batch's trials apart.
# MAGIC * Not `MlflowSparkStudy`: measured 21 min vs ~2 min for the same 96 trials (MLflow round trips). See docs/decisions.md.
# MAGIC * Validation: 3 expanding time windows before 2026 (2026 is never touched); metric = mean AP on SIU labels.
# MAGIC * Trial 0 = current defaults; the feature set (EDA-selected vs all) is a search dimension. MLflow: parent + child runs.

# COMMAND ----------

# MAGIC %pip install -q optuna

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_mkr_catalog")
dbutils.widgets.text("schema", "mkr_claims")
dbutils.widgets.text("n_trials", "96")
dbutils.widgets.text("batch_size", "16")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
n_trials, batch_size = int(dbutils.widgets.get("n_trials")), int(dbutils.widgets.get("batch_size"))
target = f"`{catalog}`.`{schema}`"

import json
import time
import mlflow
import numpy as np
import optuna
import pandas as pd

CATEGORICAL = ["peril", "product", "state"]
fa = spark.table(f"{target}.ml_feature_analysis").toPandas()
FEATURE_SETS = {"all": fa[fa.decision != "exclude"].feature.tolist(), "selected": fa[fa.decision == "keep"].feature.tolist()}
FOLDS = [("2024-07-01", "2025-01-01"), ("2025-01-01", "2025-07-01"), ("2025-07-01", "2026-01-01")]

data = spark.table(f"{target}.gold_claim_features").where("claim_status = 'closed' AND report_date < '2026-01-01'").toPandas()
data["has_contractor"] = data.contractor_id.notna()
for c in ["storm_matched", "public_adjuster", "has_contractor"]:
    data[c] = data[c].map({True: 1.0, False: 0.0})
num_cols = [f for f in FEATURE_SETS["all"] if f not in CATEGORICAL]
data[num_cols] = data[num_cols].astype(float)
data["report_date"] = pd.to_datetime(data.report_date)
data = data[FEATURE_SETS["all"] + ["report_date", "label_confirmed_fraud"]]
print({k: len(v) for k, v in FEATURE_SETS.items()}, len(data), "training claims")

# COMMAND ----------

def run_trials(batches):
    """Runs on Spark workers: cross-validates each config in the partition over the time windows."""
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.metrics import average_precision_score, roc_auc_score
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OrdinalEncoder
    for batch in batches:
        out = []
        for row in batch.itertuples():
            p = json.loads(row.params)
            feats = FEATURE_SETS[p["feature_set"]]
            cats = [c for c in CATEGORICAL if c in feats]
            t0, aps, aucs = time.time(), [], []
            for start, end in FOLDS:
                fit = data[data.report_date < start]
                val = data[(data.report_date >= start) & (data.report_date < end)]
                model = Pipeline([
                    ("encode", ColumnTransformer([("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), cats)],
                                                 remainder="passthrough", verbose_feature_names_out=False)),
                    ("gbt", HistGradientBoostingClassifier(
                        categorical_features=list(range(len(cats))), learning_rate=p["learning_rate"], max_iter=p["max_iter"],
                        max_leaf_nodes=p["max_leaf_nodes"], min_samples_leaf=p["min_samples_leaf"],
                        l2_regularization=p["l2_regularization"], random_state=0,
                        class_weight=None if p["class_weight"] == "none" else p["class_weight"])),
                ]).fit(fit[feats], fit.label_confirmed_fraud.astype(int))
                pred = model.predict_proba(val[feats])[:, 1]
                aps.append(average_precision_score(val.label_confirmed_fraud, pred))
                aucs.append(roc_auc_score(val.label_confirmed_fraud, pred))
            out.append({"trial_id": row.trial_id, "mean_ap": float(np.mean(aps)), "std_ap": float(np.std(aps)),
                        "mean_auc": float(np.mean(aucs)), "fold_aps": json.dumps([round(a, 4) for a in aps]),
                        "fit_seconds": time.time() - t0})
        yield pd.DataFrame(out)


def suggest(trial):
    return {"learning_rate": trial.suggest_float("learning_rate", 0.02, 0.3, log=True),
            "max_iter": trial.suggest_categorical("max_iter", [100, 200, 300, 500]),
            "max_leaf_nodes": trial.suggest_categorical("max_leaf_nodes", [7, 15, 31, 63]),
            "min_samples_leaf": trial.suggest_categorical("min_samples_leaf", [10, 20, 50, 100]),
            "l2_regularization": trial.suggest_categorical("l2_regularization", [0.0, 0.1, 1.0, 5.0]),
            "class_weight": trial.suggest_categorical("class_weight", ["balanced", "none"]),
            "feature_set": trial.suggest_categorical("feature_set", ["selected", "all"])}


optuna.logging.set_verbosity(optuna.logging.WARNING)
study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=7, constant_liar=True))
study.enqueue_trial({"learning_rate": 0.05, "max_iter": 300, "max_leaf_nodes": 15, "min_samples_leaf": 20,
                     "l2_regularization": 1.0, "class_weight": "balanced", "feature_set": "all"})  # trial 0: current defaults
schema_out = "trial_id long, mean_ap double, std_ap double, mean_auc double, fold_aps string, fit_seconds double"
batches, done, t0 = [], 0, time.time()
while done < n_trials:
    asked = [study.ask() for _ in range(min(batch_size, n_trials - done))]
    configs = pd.DataFrame({"trial_id": [t.number for t in asked], "params": [json.dumps(suggest(t)) for t in asked]})
    out = spark.createDataFrame(configs).repartition(len(asked)).mapInPandas(run_trials, schema_out).toPandas()
    out = out.merge(configs, on="trial_id").assign(batch=len(batches))
    for t in asked:  # tell the sampler before it proposes the next batch
        study.tell(t, float(out.loc[out.trial_id == t.number, "mean_ap"].iloc[0]))
    batches.append(out)
    done += len(asked)
    print(f"batch {len(batches)}: best so far {study.best_value:.3f}")
wall = time.time() - t0
res = pd.concat(batches).sort_values("mean_ap", ascending=False).reset_index(drop=True)
compute = res.fit_seconds.sum()
print(f"{len(res)} trials x {len(FOLDS)} folds = {len(res) * len(FOLDS)} fits | fit {compute:.0f}s | wall {wall:.0f}s | "
      f"{compute / wall:.1f}x fit time per wall second")

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/mkr_claims_fraud_{schema}")
client = mlflow.MlflowClient()
with mlflow.start_run(run_name="hyperparam_search") as parent:
    for r in res.itertuples():
        child = client.create_run(parent.info.experiment_id, run_name=f"trial_{r.trial_id}",
                                  tags={"mlflow.parentRunId": parent.info.run_id})
        client.log_batch(child.info.run_id,
                         params=[mlflow.entities.Param(k, str(v)) for k, v in json.loads(r.params).items()],
                         metrics=[mlflow.entities.Metric(m, getattr(r, m), int(time.time() * 1000), 0)
                                  for m in ("mean_ap", "std_ap", "mean_auc")])
        client.set_terminated(child.info.run_id)
    best, baseline = res.iloc[0], res[res.trial_id == 0].iloc[0]
    summary = {"n_trials": len(res), "batch_size": batch_size, "fits": len(res) * len(FOLDS), "fit_seconds": compute,
               "wall_seconds": wall, "best_trial": int(best.trial_id), "best_mean_ap": best.mean_ap,
               "best_std_ap": best.std_ap, "baseline_mean_ap": baseline.mean_ap, "baseline_rank": int(baseline.name) + 1}
    mlflow.log_metrics({k: float(v) for k, v in summary.items()})
    mlflow.log_dict(json.loads(best.params), "best_config.json")

search_id = parent.info.run_id
spark.createDataFrame(res.assign(search_id=search_id, rank=res.index + 1, search_wall_seconds=wall)) \
    .selectExpr("*", "current_timestamp() AS searched_at") \
    .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{target}.ml_tuning_trials")
best_config = {**json.loads(best.params), "features": FEATURE_SETS[json.loads(best.params)["feature_set"]],
               "search_id": search_id, "trial_id": int(best.trial_id)}
dbutils.jobs.taskValues.set("best_config", json.dumps(best_config))
print(json.dumps(summary, indent=2, default=float))
display(spark.createDataFrame(res.head(10)))
