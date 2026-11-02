# Databricks notebook source
# MAGIC %md
# MAGIC # Governance: classification tags
# MAGIC Runs **after** the pipeline. PII columns get Databricks' built-in data classification tags (`class.*`),
# MAGIC and tables get owner, medallion-layer and PII flags, so the catalog is searchable and tag-based policies
# MAGIC can target the data. The metastore enforces governed tag policies (e.g. `domain` only allows fixed values),
# MAGIC so PII uses the standard key-only `class.*` tags, layer and PII flags use the existing governed `medallion_layer` and `contains_pii` tags, and ownership uses our own `mkr_owner_team` key.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_mkr_catalog")
dbutils.widgets.text("schema", "mkr_claims")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
target = f"`{catalog}`.`{schema}`"

# COMMAND ----------

PII_COLUMNS = {  # table -> {column: built-in classification tag}
    "silver_policyholders": {"full_name": "class.name", "email": "class.email_address",
                             "phone": "class.phone_number", "street_address": "class.location"},
    "bronze_policyholders": {"full_name": "class.name", "email": "class.email_address",
                             "phone": "class.phone_number", "street_address": "class.location",
                             "date_of_birth": "class.date_of_birth"},
}

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

# COMMAND ----------

display(spark.sql(f"""
SELECT table_name, column_name, tag_name, tag_value
FROM `{catalog}`.information_schema.column_tags
WHERE schema_name = '{schema}' ORDER BY table_name, column_name
"""))
