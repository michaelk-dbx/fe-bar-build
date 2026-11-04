# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog governance for the claims data products
# MAGIC Runs twice in the build job:
# MAGIC * **`phase=setup`** (before the pipeline): creates the PII column-mask functions that `silver_policyholders`
# MAGIC   attaches to its columns. Members of the SIU group see real values; everyone else (claims analysts, Genie users,
# MAGIC   the app) sees masked values.
# MAGIC * **`phase=tags`** (after scoring): tags PII columns with Databricks' built-in data classification tags (`class.*`)
# MAGIC   and tags tables with owner, medallion layer and PII flag. The metastore enforces governed tag policies (e.g.
# MAGIC   `domain` only allows fixed values), so layer and PII flags use the existing governed `medallion_layer` /
# MAGIC   `contains_pii` tags, and ownership uses our own `mkr_owner_team` key.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_mkr_catalog")
dbutils.widgets.text("schema", "mkr_claims")
dbutils.widgets.dropdown("phase", "setup", ["setup", "tags"])
dbutils.widgets.text("siu_group", "mkr_siu")
catalog, schema, phase, siu_group = (dbutils.widgets.get(k) for k in ("catalog", "schema", "phase", "siu_group"))
target = f"`{catalog}`.`{schema}`"
print(f"phase={phase} | target schema: {catalog}.{schema} | unmasked group: {siu_group}")

# COMMAND ----------

if phase == "setup":
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

# COMMAND ----------

PII_COLUMNS = {  # table -> {column: built-in classification tag}
    "silver_policyholders": {"full_name": "class.name", "email": "class.email_address",
                             "phone": "class.phone_number", "street_address": "class.location"},
    "bronze_policyholders": {"full_name": "class.name", "email": "class.email_address",
                             "phone": "class.phone_number", "street_address": "class.location",
                             "date_of_birth": "class.date_of_birth"},
}

if phase == "tags":
    for t in spark.sql(f"SHOW TABLES IN {target}").collect():
        name = t.tableName
        layer = name.split("_")[0]
        if layer not in ("bronze", "silver", "gold"):
            continue
        is_mv = spark.sql(f"DESCRIBE TABLE EXTENDED {target}.`{name}`").filter("col_name = 'Type'").first().data_type == "MATERIALIZED_VIEW"
        kind = "MATERIALIZED VIEW" if is_mv else "TABLE"
        spark.sql(f"ALTER {kind} {target}.`{name}` SET TAGS ('mkr_owner_team' = 'claims_data', "
                  f"'medallion_layer' = '{layer}', 'contains_pii' = '{str(name in PII_COLUMNS).lower()}')")
        for col, tag in PII_COLUMNS.get(name, {}).items():
            spark.sql(f"ALTER {kind} {target}.`{name}` ALTER COLUMN `{col}` SET TAGS ('{tag}')")
        print(f"tagged {kind.lower():17} {name}")
    display(spark.sql(f"""
    SELECT table_name, column_name, tag_name FROM `{catalog}`.information_schema.column_tags
    WHERE schema_name = '{schema}' ORDER BY table_name, column_name
    """))
