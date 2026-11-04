# MKR & Co. — Smart Property Claims Triage & Fraud Prevention

> **Business outcome:** MKR & Co. (a fictional US homeowners insurer, 50k policies, ~$34M claims paid in 2026 YTD) lets fraud through because its Special Investigations Unit (SIU) works a list picked by static rules: only **23.6%** of referrals are real fraud. This build scores every claim as it arrives. **With the same SIU capacity, it refers 2.5x more real fraud (106 vs 42 frauds, 59.6% precision), cutting fraud paid out by $1.83M in Jan–Oct 2026** (≈$2.2M/yr, ~5% of claims paid). It also **fast-tracks 45% of claims** (fraud rate 1.2%, ≤ $25k) so honest policyholders get paid faster. Every referral comes with plain-English reasons and an AI triage note.
>
> _Figures come from the 2026 test period of the [model evaluation](evidence/03_fraud_model_and_genai.md). The data is synthetic, so the hidden ground truth makes leakage measurable._

**Industry Outcome Map:** P&C Insurance → *Claims & Fraud Prevention* (Claims Ingestion, Smart Claims Triage, Claims Analytics, Fraud Models), plus *Dynamic Feedback Loop*.

**Who it's for:** the VP of Claims (loss ratio, fraud leakage, cycle time), SIU leadership (hit rate), and adjusters (a prioritised queue with reasons, not a black-box score).

## The data journey
Source-system exports → **Lakeflow** (bronze/silver/gold) → **Unity Catalog** (governance, PII masking) → **ML fraud model + GenAI triage notes** → **Lakebase** (live triage queue + adjuster decisions) → **Genie** (claims analytics in natural language) → **Databricks App** (FastAPI + React adjuster workbench)

| Stage | What it does | Evidence |
|---|---|---|
| 1. Source data | Synthetic policy admin, contractors, weather, FNOL claims and SIU outcomes landed in a UC volume | ✅ [01](evidence/01_landing_zone.md) |
| 2. Lakeflow + UC | SQL declarative pipeline: Auto Loader, expectations, AUTO CDC dedupe, point-in-time features; PII masks and classification tags | ✅ [02](evidence/02_lakeflow_pipeline.md) · [02b](evidence/02b_unity_catalog_governance.md) |
| 3. ML + GenAI | Gradient-boosted fraud model in MLflow/UC, serving endpoint, risk tiers + reason codes, `ai_query` triage notes | ✅ [03](evidence/03_fraud_model_and_genai.md) |
| 4. Lakebase | Triage queue served from Postgres; adjuster decisions written back | ⏳ |
| 5. Genie | Natural-language claims analytics over the gold tables | ⏳ |
| 6. App | Adjuster workbench: queue, claim detail, decisions, Genie | ⏳ |

Everything is deployed with one Asset Bundle and runs as one job, `mkr_claims_build`: `governance_setup` → `lakeflow_pipeline` → `train_fraud_model` → (`deploy_serving`, `score_claims`) → `governance_tags`.

## Key tables (`serverless_stable_mkr_catalog.mkr_claims`)
| Table | Grain | Used by |
|---|---|---|
| `gold_claim_features` | claim | model training/scoring, Genie |
| `gold_claim_scores` | claim | queue: `fraud_score`, `risk_score` (0–100), `risk_tier`, `reason_codes` |
| `gold_claim_ai_triage` | open claim | LLM summary, narrative red flags, next step |
| `gold_contractor_risk` | contractor | SIU ring detection, Genie |
| `gold_claims_kpis_monthly` | month × state × peril | claims leadership, Genie |

## The data
`src/data_gen/generate.py` simulates the source systems, with six known property-fraud patterns: claims soon after a policy starts or coverage is raised, hail/wind claims with no storm on record, a ring of 14 contractors, inflated amounts, late reporting, and red-flag narratives. Legitimate claims share some of these traits, so no single rule catches the fraud. The FNOL feed is messy on purpose: ~1% of claims are resubmitted and 15 records are malformed.

## Decisions & trade-offs
See **[docs/decisions.md](docs/decisions.md)**. The most important ones:
- The model trains on **SIU-confirmed labels** (what the insurer actually knows), not ground truth, and is tested on a later time period.
- **Thresholds match today's SIU capacity**, so the value comes from better referrals, not more investigators.
- **Reason codes are deterministic and the LLM only explains.** Decisions stay auditable.
- **PII is masked by Unity Catalog** for everyone outside the SIU group, including Genie and the app.

## How AI was used
- Claude Code (Opus) was a pair-programmer for design, pipeline SQL, notebooks, bundle config and debugging. Every result was checked against expected numbers before being committed.
- **Checks caught AI-introduced bugs.** (1) The first pipeline run dropped only 10 of 15 malformed records, and 45% of clean claims showed a "contractor with prior fraud". The generator was writing pandas `NaN` as the string `"NaN"`, which merged every claim without a contractor into one fake contractor; fixed at the source (5/5/5 drops, 3.6% vs 13.6%). (2) `ai_query` with structured output failed on Claude endpoints; four endpoints were tested and the one that guarantees the schema was picked.
- A local experiment checked that SIU-only labels still give a useful model (2.5x more fraud referred) before the training notebook was written.

## Repo layout
| Path | Contents |
|---|---|
| `src/data_gen/` | Synthetic source-system generator + `land.sh` to load a target's volumes |
| `src/pipelines/` | Lakeflow Spark Declarative Pipeline (SQL) |
| `src/notebooks/` | Governance, model training, serving, scoring + GenAI |
| `src/evidence/` | `sql_evidence.py` + query files that produce `evidence/` |
| `src/app/` | Databricks App (FastAPI + React) |
| `resources/` | Asset Bundle resources (schema, volumes, pipeline, job) |
| `docs/` | Decisions & trade-offs |
| `evidence/` | Text execution evidence |
| `deck/` | Presentation deck |
