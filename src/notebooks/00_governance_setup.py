# Databricks notebook source
# MAGIC %md
# MAGIC # Governance setup: PII column-mask functions
# MAGIC Runs **before** the pipeline, because `silver_policyholders` attaches these masks to its columns.
# MAGIC Members of the SIU group see real values; everyone else (claims analysts, Genie users, the app) sees masked values.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_mkr_catalog")
dbutils.widgets.text("schema", "mkr_claims")
dbutils.widgets.text("siu_group", "mkr_siu")
catalog, schema, siu_group = (dbutils.widgets.get(k) for k in ("catalog", "schema", "siu_group"))
target = f"`{catalog}`.`{schema}`"
print(f"target schema: {catalog}.{schema} | unmasked group: {siu_group}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {target}.mask_pii(value STRING)
RETURNS STRING
COMMENT 'Column mask: full value for the {siu_group} group, redacted for everyone else'
RETURN CASE WHEN is_account_group_member('{siu_group}') THEN value ELSE '***REDACTED***' END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {target}.mask_phone(value STRING)
RETURNS STRING
COMMENT 'Column mask: full phone for the {siu_group} group, last 4 digits for everyone else'
RETURN CASE WHEN is_account_group_member('{siu_group}') THEN value ELSE concat('***-***-', right(value, 4)) END
""")

display(spark.sql(f"SHOW USER FUNCTIONS IN {target}"))
