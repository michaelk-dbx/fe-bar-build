# FE Bar Build — <industry>: <customer problem>

> **Business outcome:** _TBD — lead with the problem and the quantified value (e.g. "$X M/yr")._

## The data journey
Raw data → **Lakeflow** (bronze/silver/gold) → **Unity Catalog** (governance) → **Lakebase** (operational serving) → **ML / GenAI** → **Genie** (NL queries) → **Databricks App** (business users)

## Execution evidence
See [`evidence/`](evidence/) — committed run outputs, query results and model output (text).

## Decisions & trade-offs
_TBD_

## How AI was used
_TBD — tools, prompts, patterns._

## Repo layout
| Path | Contents |
|---|---|
| `src/pipelines/` | Lakeflow Spark Declarative Pipeline |
| `src/notebooks/` | Data generation, ML training, Lakebase sync |
| `src/app/` | Databricks App |
| `resources/` | Asset Bundle resource definitions |
| `evidence/` | Text execution evidence |
| `deck/` | Presentation deck (md/pdf) |
