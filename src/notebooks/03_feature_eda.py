# Databricks notebook source
# MAGIC %md
# MAGIC # Feature EDA and selection
# MAGIC Which candidate features carry fraud signal, which are redundant, and which must never be used?
# MAGIC Uses the **training period only** (closed claims reported before 2026), so the 2026 test set never influences selection.
# MAGIC
# MAGIC | Check | Method | Rule |
# MAGIC |---|---|---|
# MAGIC | Leakage | Domain review | Exclude anything only known after the claim closes (`siu_*`, `paid_amount`, `claim_status`) |
# MAGIC | Coverage | Missing rate, fraud rate when missing | Missing is kept as a signal (gradient-boosted trees route it) |
# MAGIC | Univariate signal | Single-feature AUC, mutual information | Weak if AUC < 0.56 |
# MAGIC | Redundancy | Spearman correlation | Drop if \|rho\| ≥ 0.95 with a feature the model relies on more |
# MAGIC | Multivariate value | Permutation importance (average precision) on 2025, model fit on 2023–24 | Weak if importance ≤ its std (indistinguishable from 0) |
# MAGIC | Domain override | Known property-fraud indicators | Kept even if weak: with only ~70 labels the data can't prove their value yet |
# MAGIC
# MAGIC Output: `ml_feature_analysis`, one row per feature with its evidence and keep/drop decision. The hyperparameter search
# MAGIC then tests the selected set against all features, so selection is validated rather than assumed.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_mkr_catalog")
dbutils.widgets.text("schema", "mkr_claims")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
target = f"`{catalog}`.`{schema}`"

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.feature_selection import mutual_info_classif
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

NUMERIC = ["days_since_policy_start", "days_since_coverage_increase", "report_lag_days", "claimed_amount",
           "amount_to_coverage_ratio", "amount_per_sqft", "amount_vs_peril_median", "storm_matched",
           "matched_storm_severity", "contractor_prior_claims", "contractor_prior_confirmed_fraud",
           "policy_prior_claims_3y", "public_adjuster", "has_contractor", "dwelling_coverage", "deductible",
           "square_feet", "year_built", "roof_age_years"]
CATEGORICAL = ["peril", "product", "state"]
LEAKAGE = ["siu_referred", "siu_outcome", "paid_amount", "claim_status"]  # known only after the claim closes
DOMAIN_KEEP = {"days_since_coverage_increase": "inception fraud after a coverage increase",
               "contractor_prior_confirmed_fraud": "contractor fraud rings",
               "policy_prior_claims_3y": "repeat claimants",
               "storm_matched": "weather-claim verification"}

# COMMAND ----------

# Coverage and fraud rate when missing, computed in Spark (scales to the full claims history)
display(spark.sql(f"""
  SELECT count(*) AS claims, sum(label_confirmed_fraud) AS confirmed_fraud,
         round(avg(label_confirmed_fraud), 4) AS fraud_rate,
         round(avg(CAST(days_since_coverage_increase IS NULL AS INT)), 3) AS missing_coverage_increase,
         round(avg(CAST(storm_matched IS NULL AS INT)), 3) AS missing_storm_check
  FROM {target}.gold_claim_features WHERE claim_status = 'closed' AND report_date < '2026-01-01'
"""))

df = spark.table(f"{target}.gold_claim_features").where("claim_status = 'closed' AND report_date < '2026-01-01'").toPandas()
df["has_contractor"] = df.contractor_id.notna()
for c in ["storm_matched", "public_adjuster", "has_contractor"]:
    df[c] = df[c].map({True: 1.0, False: 0.0})
df[NUMERIC] = df[NUMERIC].astype(float)
df["report_date"] = pd.to_datetime(df.report_date)
y = df.label_confirmed_fraud.astype(int)

# COMMAND ----------

# Univariate signal
rows = []
for f in NUMERIC + CATEGORICAL:
    if f in CATEGORICAL:
        x = df.groupby(f).label_confirmed_fraud.transform("mean")  # target rate per category (training data only)
        mi = mutual_info_classif(df[f].astype("category").cat.codes.to_frame(), y, discrete_features=True, random_state=0)[0]
        missing, missing_fraud_rate = 0.0, float("nan")
    else:
        missing = df[f].isna().mean()
        missing_fraud_rate = y[df[f].isna()].mean() if missing else float("nan")
        x = df[f].fillna(df[f].median() if df[f].notna().any() else 0)
        mi = mutual_info_classif(x.to_frame(), y, random_state=0)[0]
    auc = roc_auc_score(y, x)
    rows.append({"feature": f, "missing_rate": missing, "fraud_rate_when_missing": missing_fraud_rate,
                 "univariate_auc": max(auc, 1 - auc), "mutual_info": mi})
stats = pd.DataFrame(rows).set_index("feature")

# Multivariate value: permutation importance on 2025 for a model fit on 2023-24
fit, val = df[df.report_date < "2025-01-01"], df[df.report_date >= "2025-01-01"]
model = Pipeline([
    ("encode", ColumnTransformer([("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CATEGORICAL)],
                                 remainder="passthrough", verbose_feature_names_out=False)),
    ("gbt", HistGradientBoostingClassifier(categorical_features=[0, 1, 2], max_iter=300, learning_rate=0.05, max_leaf_nodes=15,
                                           min_samples_leaf=20, l2_regularization=1.0, class_weight="balanced", random_state=0)),
]).fit(fit[NUMERIC + CATEGORICAL], fit.label_confirmed_fraud.astype(int))
pi = permutation_importance(model, val[NUMERIC + CATEGORICAL], val.label_confirmed_fraud.astype(int),
                            scoring="average_precision", n_repeats=10, random_state=0)
stats["perm_importance"], stats["perm_importance_std"] = pi.importances_mean, pi.importances_std

# Redundancy
rho = df[NUMERIC].corr(method="spearman").abs()
for f in NUMERIC:
    other = rho[f].drop(f)
    stats.loc[f, "most_correlated_with"], stats.loc[f, "abs_spearman"] = other.idxmax(), other.max()

# COMMAND ----------

def decide(f, r):
    if f in NUMERIC and r.abs_spearman >= 0.95 and stats.loc[r.most_correlated_with, "perm_importance"] > r.perm_importance:
        return "drop", f"redundant with {r.most_correlated_with} (rho {r.abs_spearman:.2f}), which the model relies on more"
    weak = r.univariate_auc < 0.56 and r.perm_importance <= r.perm_importance_std
    if weak and f in DOMAIN_KEEP:
        return "keep", f"weak in this data but kept as a known indicator of {DOMAIN_KEEP[f]}"
    if weak:
        return "drop", "no univariate signal and no measurable value to the model"
    return "keep", "carries signal"

stats[["decision", "reason"]] = [decide(f, r) for f, r in stats.iterrows()]
out = stats.reset_index().sort_values("perm_importance", ascending=False)
out = pd.concat([out, pd.DataFrame([{"feature": f, "decision": "exclude", "reason": "target leakage: known only after the claim closes"}
                                    for f in LEAKAGE])])
for c in ["most_correlated_with", "decision", "reason"]:
    out[c] = out[c].astype(object).where(out[c].notna(), None)
spark.createDataFrame(out) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target}.ml_feature_analysis")
print(f"keep {int((out.decision == 'keep').sum())} | drop {int((out.decision == 'drop').sum())} | exclude {len(LEAKAGE)}")
display(spark.table(f"{target}.ml_feature_analysis"))
