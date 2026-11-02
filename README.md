# MKR & Co. — Smart Property Claims Triage & Fraud Prevention

> **Business outcome (draft):** MKR & Co., a fictional US homeowners insurer with 50k policies, pays out about **$10.3M in undetected fraud** over its claims history (about 9.6% of the $107M paid). Its SIU team investigates claims picked by static rules, and only **18.4% of referrals turn out to be fraud**. This build scores every new claim as it arrives, fast-tracks the clean ones, and sends high-risk claims to SIU with a written reason. _Impact figures are filled in after the model is evaluated._

**Industry Outcome Map:** P&C Insurance → *Claims & Fraud Prevention* (Claims Ingestion, Smart Claims Triage, Claims Analytics, Fraud Models), plus *Dynamic Feedback Loop*.

## The data journey
Source-system exports → **Lakeflow** (bronze/silver/gold) → **Unity Catalog** (governance, PII masking) → **ML fraud model + LLM red-flag extraction** → **Lakebase** (live triage queue + adjuster decisions) → **Genie** (claims analytics in natural language) → **Databricks App** (FastAPI + React adjuster workbench)

| Stage | Status |
|---|---|
| 1. Synthetic source data landed in UC volume | ✅ [evidence](evidence/01_landing_zone.md) |
| 2. Lakeflow pipeline + UC governance | ⏳ |
| 3. Fraud model (MLflow, served) + LLM summaries | ⏳ |
| 4. Lakebase queue + decision write-back | ⏳ |
| 5. Genie space | ⏳ |
| 6. Adjuster app | ⏳ |

## The data
`src/data_gen/generate.py` simulates MKR & Co.'s source systems: policy admin (policyholders, policies), vendors (contractors), a NOAA-style weather events feed, first-notice-of-loss (FNOL) claims arriving as monthly JSON files, and legacy SIU outcomes. Six known property-fraud patterns are planted: claims soon after a policy starts or coverage is raised, hail/wind claims with no storm on record, a ring of 14 contractors, inflated amounts, late reporting, and red-flag narratives. Legitimate claims share some of these traits, so no single rule catches the fraud.

## Decisions & trade-offs
- **Synthetic data, not a public dataset.** Public insurance fraud datasets are small, auto-only and lack free-text notes or weather. Synthetic data lets us plant realistic property-fraud patterns, and the hidden ground truth lets us measure *leakage*, which a real insurer can't see because undetected fraud is never labeled.
- **Labels come from SIU outcomes, not ground truth.** The model trains on what MKR & Co. actually knows (confirmed fraud from past SIU investigations), including the bias that only referred claims get investigated. The ground truth is used **only** to evaluate the model, and it lives in a separate volume the pipeline never reads.
- **Data generated locally, landed as files.** This mirrors how source systems drop exports into a landing zone, so Lakeflow ingests files incrementally the way it would in production.
- **Separate dev and prod targets.** `dev` uses bundle development mode, so each developer gets an isolated `dev_<user>_mkr_claims` schema, with prefixed jobs and pipelines and paused schedules. `prod` deploys the unprefixed `mkr_claims` schema that the demo and the committed evidence run on. Everything downstream references the schema through `${resources.schemas.mkr_claims.name}`, so the same code works in both.

## How AI was used
- Claude Code (Opus) was used as a pair-programmer for the data generator design, bundle scaffolding and debugging. _(Expanded as the build progresses.)_

## Repo layout
| Path | Contents |
|---|---|
| `src/data_gen/` | Synthetic source-system generator + `land.sh` to load a target's volumes |
| `src/pipelines/` | Lakeflow Spark Declarative Pipeline |
| `src/notebooks/` | ML training, scoring, Lakebase sync |
| `src/app/` | Databricks App (FastAPI + React) |
| `resources/` | Asset Bundle resources (schema, volumes, pipeline, jobs, app) |
| `evidence/` | Text execution evidence |
| `deck/` | Presentation deck |
