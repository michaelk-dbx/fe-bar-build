# Step 4: Lakebase — operational serving and decision write-back

**What ran:** the bundle (`resources/lakebase.yml`) declares Lakebase Autoscaling project `mkr-claims` (Postgres 17, endpoint `primary` scaling 0.5–2 CU, suspends after 5 min idle) and two snapshot synced tables, `gold_claim_scores` → `claim_scores_synced` and `gold_claim_ai_triage` → `claim_ai_triage_synced`, in Postgres database `claims`. In prod job `mkr_claims_build`, the tasks `sync_claim_scores` and `sync_claim_ai_triage` refresh them after scoring. `08_lakebase` then creates the app's write-back table `app.claim_decisions` (replica identity FULL), checks the synced tables against UC, and times the adjuster queue query on Postgres. It writes a labelled smoke-test decision and waits for it to arrive in UC through Lakebase CDF (schema `app` → Delta `lb_claim_decisions_history`). **Result:** row parity 7,340/7,340 and 188/188; the queue query takes **6.2 ms p50 / 8.4 ms p95**; a decision written in Postgres is queryable in UC Delta **within 8 s**. Run ID `64988909019049` · result **SUCCESS**. The first attempt of `lakebase` aborted with SIGABRT before running any cell; the automatic retry passed.

Target: `serverless_stable_mkr_catalog.mkr_claims` · captured 2026-11-05 15:31 UTC on SQL warehouse `22d236430dbb5df0`

## Lakebase checks from the build job (lakebase_checks, latest run)

Row parity of the synced tables, adjuster-queue latency measured against Postgres, and the decision round trip through Lakebase CDF.

```sql
SELECT check, value, detail, passed FROM serverless_stable_mkr_catalog.mkr_claims.lakebase_checks
WHERE checked_at = (SELECT max(checked_at) FROM serverless_stable_mkr_catalog.mkr_claims.lakebase_checks) ORDER BY check
```

Statement `01f1d92d-fd8f-1132-9d84-aaceca569049` · 5 row(s)

| check | value | detail | passed |
|---|---|---|---|
| CDF write -> UC seconds | 8 | CLM0007189 seen in lb_claim_decisions_history (polled every 10 s) | true |
| queue query p50 ms | 6.2 | 50 rows, 30 runs, from serverless | true |
| queue query p95 ms | 8.4 |  | true |
| rows claim_ai_triage_synced | 188 | UC gold_claim_ai_triage: 188 | true |
| rows claim_scores_synced | 7,340 | UC gold_claim_scores: 7340 | true |

## The queue read back from Lakebase through Unity Catalog (synced table)

claim_scores_synced is the Postgres table, exposed in UC; open claims by tier as the app sees them.

```sql
SELECT risk_tier, count(*) AS open_claims, round(avg(risk_score)) AS avg_risk_score, round(sum(claimed_amount)) AS claimed_usd
FROM serverless_stable_mkr_catalog.mkr_claims.claim_scores_synced WHERE claim_status = 'open' GROUP BY ALL ORDER BY avg_risk_score DESC
```

Statement `01f1d92d-fe5f-1638-b01d-33fa1aaaa4db` · 3 row(s)

| risk_tier | open_claims | avg_risk_score | claimed_usd |
|---|---|---|---|
| refer_siu | 19 | 96 | 782,006 |
| adjuster_review | 117 | 66 | 2,698,960 |
| fast_track | 52 | 26 | 468,761 |

## Decision write-back: Postgres change history landed in UC Delta (Lakebase CDF)

```sql
SELECT _pg_change_type, _timestamp, claim_id, decision, adjuster, model_risk_tier, decided_at
FROM serverless_stable_mkr_catalog.mkr_claims.lb_claim_decisions_history ORDER BY _pg_lsn DESC LIMIT 6
```

Statement `01f1d92e-003d-1c56-bf4d-4479fe20aadd` · 1 row(s)

| _pg_change_type | _timestamp | claim_id | decision | adjuster | model_risk_tier | decided_at |
|---|---|---|---|---|---|---|
| insert | 2026-11-05T15:24:29.311 | CLM0007189 | refer_siu | smoke-test | refer_siu | 2026-11-05T15:24:23.771Z |

## Latest decision per claim (gold_adjuster_decisions, for Genie, SIU reporting and future labels)

```sql
SELECT claim_id, decision, adjuster, model_risk_tier, model_risk_score, decided_at, is_smoke_test
FROM serverless_stable_mkr_catalog.mkr_claims.gold_adjuster_decisions ORDER BY decided_at DESC LIMIT 5
```

Statement `01f1d92e-0128-1abf-83bb-f403fe40031b` · 1 row(s)

| claim_id | decision | adjuster | model_risk_tier | model_risk_score | decided_at | is_smoke_test |
|---|---|---|---|---|---|---|
| CLM0007189 | refer_siu | smoke-test | refer_siu | 99 | 2026-11-05T15:24:23.771Z | true |
