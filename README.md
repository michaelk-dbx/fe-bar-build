# MKR & Co. — Smart Property Claims Triage & Fraud Prevention

> **Business outcome (draft):** MKR & Co., a fictional US homeowners insurer with 50k policies, pays out about **$10.3M in undetected fraud** over its claims history (about 9.6% of the $107M paid). Its SIU team investigates claims picked by static rules, and only **18.4% of referrals turn out to be fraud**. This build scores every new claim as it arrives, fast-tracks the clean ones, and sends high-risk claims to SIU with a written reason. _Impact figures are filled in after the model is evaluated._

**Industry Outcome Map:** P&C Insurance → *Claims & Fraud Prevention* (Claims Ingestion, Smart Claims Triage, Claims Analytics, Fraud Models), plus *Dynamic Feedback Loop*.

## The data journey
Source-system exports → **Lakeflow** (bronze/silver/gold) → **Unity Catalog** (governance, PII masking) → **ML fraud model + LLM red-flag extraction** → **Lakebase** (live triage queue + adjuster decisions) → **Genie** (claims analytics in natural language) → **Databricks App** (FastAPI + React adjuster workbench)

| Stage | Status |
|---|---|
| 1. Synthetic source data landed in UC volume | ✅ [evidence](evidence/01_landing_zone.md) |
| 2. Lakeflow pipeline + UC governance | ✅ [pipeline](evidence/02_lakeflow_pipeline.md) · [governance](evidence/02b_unity_catalog_governance.md) |
| 3. Fraud model (MLflow, served) + LLM summaries | ⏳ |
| 4. Lakebase queue + decision write-back | ⏳ |
| 5. Genie space | ⏳ |
| 6. Adjuster app | ⏳ |

## The data
`src/data_gen/generate.py` simulates MKR & Co.'s source systems: policy admin (policyholders, policies), vendors (contractors), a NOAA-style weather events feed, first-notice-of-loss (FNOL) claims arriving as monthly JSON files, and legacy SIU outcomes. Six known property-fraud patterns are planted: claims soon after a policy starts or coverage is raised, hail/wind claims with no storm on record, a ring of 14 contractors, inflated amounts, late reporting, and red-flag narratives. Legitimate claims share some of these traits, so no single rule catches the fraud. The FNOL feed is messy on purpose, as real feeds are: about 1% of claims are resubmitted, and 15 records are malformed.

## Lakeflow pipeline (`src/pipelines/`, run by job `mkr_claims_build`)
| Layer | Tables | What happens |
|---|---|---|
| Bronze | `bronze_*` (6 streaming tables) | Auto Loader (`read_files`) ingests each landing folder incrementally, with source file and ingest time |
| Silver | `silver_claims_validated` → `silver_claims`, `silver_policies`, `silver_policyholders`, `silver_contractors`, `silver_weather_event_areas`, `silver_claim_outcomes` | Typing; expectations drop malformed claims (counted in the event log); AUTO CDC collapses resubmissions to the latest version; PII column masks; weather events exploded to ZIP3 areas |
| Gold | `gold_claim_features` (03), `gold_contractor_risk`, `gold_claims_kpis_monthly` (04) | Point-in-time fraud features and SIU labels per claim; contractor scorecard; monthly claims KPIs |

The job runs `00_governance_setup` (mask functions) → the pipeline → `02_governance_apply` (classification tags).

## Decisions & trade-offs
- **Synthetic data, not a public dataset.** Public insurance fraud datasets are small, auto-only and lack free-text notes or weather. Synthetic data lets us plant realistic property-fraud patterns, and the hidden ground truth lets us measure *leakage*, which a real insurer can't see because undetected fraud is never labeled.
- **Labels come from SIU outcomes, not ground truth.** The model trains on what MKR & Co. actually knows (confirmed fraud from past SIU investigations), including the bias that only referred claims get investigated. The ground truth is used **only** to evaluate the model, and it lives in a separate volume the pipeline never reads.
- **Data generated locally, landed as files.** This mirrors how source systems drop exports into a landing zone, so Lakeflow ingests files incrementally the way it would in production.
- **SQL for the pipeline.** Declarative SQL keeps the business logic (e.g. "a hail claim must match a storm in the same ZIP3 within 3 days") readable by claims analysts, not just engineers.
- **Streaming tables for feeds, materialized views for snapshots.** FNOL claims arrive as new monthly files, so they're ingested incrementally. Policy, contractor and weather exports are full snapshots, so MVs recompute them. In production the policy feed would come through CDC.
- **Drop and count bad records, rather than failing the pipeline.** One malformed FNOL record shouldn't stop the whole triage queue. Expectations drop it and the event log counts it (5 per rule in this run), so data quality is visible and can be alerted on.
- **AUTO CDC for resubmissions.** Insurers routinely get updated FNOLs. Keying on `claim_id` and sequencing by `fnol_version` keeps the latest version, without double-counting exposure.
- **Point-in-time features.** Contractor fraud history and prior claims only count what was known *before* the claim was reported. Otherwise the model would learn from SIU outcomes that didn't exist yet at triage time (target leakage).
- **Govern PII at silver, minimise it.** Email, phone and address are masked by Unity Catalog column masks unless the caller is in the SIU group, so Genie users, analysts and the app see masked values with no extra code. Date of birth is reduced to birth year. In production the `mkr_siu` group would be synced from MKR & Co.'s identity provider so SIU investigators see real values. The demo runs as a user outside that group, so it shows the masked view that analysts, Genie users and the app get. PII columns carry Databricks' built-in `class.*` classification tags, and tables use the metastore's existing governed `medallion_layer` / `contains_pii` tags rather than new ad-hoc keys.
- **Separate dev and prod targets, same schema name.** Both targets define the schema as `mkr_claims`. `dev` uses bundle development mode, which adds its default per-user prefix, so dev work lands in an isolated `dev_<user>_mkr_claims` schema with prefixed jobs and pipelines and paused schedules. `prod` deploys the unprefixed `mkr_claims` schema that the demo and the committed evidence run on. Downstream code references the schema through `${resources.schemas.mkr_claims.name}`, so the same code works in both.

## How AI was used
- Claude Code (Opus) was used as a pair-programmer for the data generator design, pipeline SQL, bundle scaffolding and debugging. _(Expanded as the build progresses.)_
- **Data quality checks caught an AI-introduced bug.** The first pipeline run showed only 10 of the 15 malformed records dropped, and 45% of clean claims linked to a "contractor with prior fraud". The cause: the generator wrote pandas `NaN` as the JSON token `NaN` instead of `null`, so missing claim IDs and contractor IDs became the string `"NaN"`, which turned every claim without a contractor into one giant fake contractor. Fixed at the source and verified (5/5/5 drops, 3.6% vs 13.6%). Checking AI-written code against expected numbers is part of the workflow.

## Repo layout
| Path | Contents |
|---|---|
| `src/data_gen/` | Synthetic source-system generator + `land.sh` to load a target's volumes |
| `src/pipelines/` | Lakeflow Spark Declarative Pipeline |
| `src/notebooks/` | Governance setup/tags; ML training, scoring, Lakebase sync |
| `src/evidence/` | `sql_evidence.py` + query files that produce the `evidence/` markdown |
| `src/app/` | Databricks App (FastAPI + React) |
| `resources/` | Asset Bundle resources (schema, volumes, pipeline, build job; later model, app) |
| `evidence/` | Text execution evidence |
| `deck/` | Presentation deck |
