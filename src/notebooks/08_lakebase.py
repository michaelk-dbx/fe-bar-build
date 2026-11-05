# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase: operational serving + decision write-back
# MAGIC * **UC -> Postgres:** `gold_claim_scores` and `gold_claim_ai_triage` arrive as synced tables (bundle resources,
# MAGIC   refreshed by the job's sync tasks). This notebook checks they match UC and times the adjuster queue query.
# MAGIC * **Postgres -> UC:** adjuster decisions are written by the app to the native table `app.claim_decisions` (one row per
# MAGIC   claim, upserted). Lakebase CDF replicates schema `app` to UC as Delta history: the feedback loop for SIU and retraining.
# MAGIC * A labelled smoke-test decision proves the round trip on every run.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.161" "psycopg[binary]>=3.2"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_mkr_catalog")
dbutils.widgets.text("schema", "mkr_claims")
dbutils.widgets.text("project", "mkr-claims")
catalog, schema, project = (dbutils.widgets.get(k) for k in ("catalog", "schema", "project"))
target = f"`{catalog}`.`{schema}`"

import statistics
import time
import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import CdfConfig

w = WorkspaceClient()
branch = f"projects/{project}/branches/production"
endpoint = f"{branch}/endpoints/primary"
host = w.postgres.get_endpoint(endpoint).status.hosts.host
conn = psycopg.connect(host=host, dbname="claims", user=w.current_user.me().user_name, sslmode="require",
                       password=w.postgres.generate_database_credential(endpoint=endpoint).token, autocommit=True)
pg = lambda sql, args=None: conn.execute(sql, args).fetchall()

# COMMAND ----------

# Write-back table owned by the app. REPLICA IDENTITY FULL is required by Lakebase CDF (full rows in the WAL).
conn.execute("CREATE SCHEMA IF NOT EXISTS app")
conn.execute("""
CREATE TABLE IF NOT EXISTS app.claim_decisions (
  claim_id         text PRIMARY KEY,
  decision         text NOT NULL CHECK (decision IN ('approve', 'refer_siu', 'request_info', 'deny')),
  adjuster         text NOT NULL,
  note             text,
  model_risk_tier  text,
  model_risk_score int,
  decided_at       timestamptz NOT NULL DEFAULT now())""")
conn.execute("ALTER TABLE app.claim_decisions REPLICA IDENTITY FULL")

# COMMAND ----------

# Synced tables vs their UC sources.
checks = []
for src, synced in [("gold_claim_scores", "claim_scores_synced"), ("gold_claim_ai_triage", "claim_ai_triage_synced")]:
    uc = spark.table(f"{target}.{src}").count()
    lb = pg(f'SELECT count(*) FROM "{schema}".{synced}')[0][0]
    checks.append((f"rows {synced}", float(lb), f"UC {src}: {uc}", lb == uc))

# The adjuster queue as the app will query it: open claims by risk, with the AI triage note.
QUEUE = f"""SELECT s.claim_id, s.peril, s.claimed_amount, s.risk_score, s.risk_tier, s.reason_codes, t.triage, d.decision
FROM "{schema}".claim_scores_synced s
LEFT JOIN "{schema}".claim_ai_triage_synced t USING (claim_id)
LEFT JOIN app.claim_decisions d USING (claim_id)
WHERE s.claim_status = 'open' ORDER BY s.risk_score DESC, s.claim_id LIMIT 50"""
ms = []
for _ in range(30):
    t0 = time.perf_counter(); rows = pg(QUEUE); ms.append((time.perf_counter() - t0) * 1000)
ms.sort()
checks += [("queue query p50 ms", round(statistics.median(ms), 1), f"{len(rows)} rows, 30 runs, from serverless", True),
           ("queue query p95 ms", round(ms[int(len(ms) * 0.95) - 1], 1), "", True)]

# COMMAND ----------

# Round trip: a labelled smoke-test decision on the top open claim, then wait for it in UC via Lakebase CDF.
claim_id, tier, score = rows[0][0], rows[0][4], rows[0][3]
stamp = pg("""INSERT INTO app.claim_decisions (claim_id, decision, adjuster, note, model_risk_tier, model_risk_score)
VALUES (%s, 'refer_siu', 'smoke-test', 'build job round-trip check, not a real decision', %s, %s)
ON CONFLICT (claim_id) DO UPDATE SET decided_at = now(), note = EXCLUDED.note RETURNING decided_at""", (claim_id, tier, score))[0][0]

db = next(d.name for d in w.postgres.list_databases(branch) if d.status and d.status.postgres_database == "claims")
if not any(c.postgres_schema == "app" for c in w.postgres.list_cdf_configs(db)):
    w.postgres.create_cdf_config(db, CdfConfig(catalog=catalog, schema=schema, postgres_schema="app"),
                                 cdf_config_id="app_to_uc").wait()

history, t0, lag = f"{target}.lb_claim_decisions_history", time.time(), None
while time.time() - t0 < 600 and lag is None:
    if spark.catalog.tableExists(history.replace("`", "")) and spark.sql(
            f"SELECT 1 FROM {history} WHERE claim_id = '{claim_id}' AND decided_at >= '{stamp.isoformat()}'").count():
        lag = time.time() - t0
    else:
        time.sleep(10)
checks.append(("CDF write -> UC seconds", round(lag or -1, 0), f"{claim_id} seen in lb_claim_decisions_history (polled every 10 s)", lag is not None))

# Latest decision per claim, for Genie, SIU reporting and future labels.
spark.sql(f"""CREATE OR REPLACE VIEW {target}.gold_adjuster_decisions AS
SELECT claim_id, decision, adjuster, note, model_risk_tier, model_risk_score, decided_at, adjuster = 'smoke-test' AS is_smoke_test
FROM (SELECT *, row_number() OVER (PARTITION BY claim_id ORDER BY _pg_lsn DESC, _pg_change_type) AS rn
      FROM {history} WHERE _pg_change_type != 'update_preimage')
WHERE rn = 1 AND _pg_change_type != 'delete'""")

# COMMAND ----------

import pandas as pd
out = pd.DataFrame(checks, columns=["check", "value", "detail", "passed"]).assign(project=project, endpoint=endpoint)
spark.createDataFrame(out).selectExpr("*", "current_timestamp() AS checked_at") \
    .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{target}.lakebase_checks")
display(out)
assert out.passed.all(), out[~out.passed]
