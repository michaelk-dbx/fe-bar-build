"""Run named SQL queries on a Databricks SQL warehouse and write the results as a markdown evidence file.

Queries live in a .sql file as blocks that start with `-- name: <title>`; an optional
`-- note: <text>` line right after it is printed under the title. {catalog} and {schema}
are substituted. Auth comes from the Databricks CLI profile (DATABRICKS_CONFIG_PROFILE).

Usage: uv run python src/evidence/sql_evidence.py queries.sql --schema mkr_claims --out evidence/02_x.md --title "..."
"""

import argparse
import json
import re
import subprocess
import time
from datetime import datetime, timezone

WAREHOUSE_ID = "22d236430dbb5df0"


def api(method, path, body=None):
    cmd = ["databricks", "api", method, path] + (["--json", json.dumps(body)] if body else [])
    out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def run(sql):
    r = api("post", "/api/2.0/sql/statements",
            {"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    return r


def parse_blocks(text):
    blocks = []
    for chunk in re.split(r"^-- name: ", text, flags=re.M)[1:]:
        title, _, body = chunk.partition("\n")
        note = None
        if body.startswith("-- note: "):
            note, _, body = body[len("-- note: "):].partition("\n")
        blocks.append((title.strip(), note, body.strip().rstrip(";")))
    return blocks


def fmt(v, col=""):
    if v is None:
        return "NULL"
    try:
        f = float(v)
    except ValueError:
        return v.replace("|", "\\|").replace("\n", " ")
    if col.endswith("year") or "_id" in col:
        return v
    return f"{f:,.0f}" if f.is_integer() else f"{f:,.4g}"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("sql_file")
    p.add_argument("--catalog", default="serverless_stable_mkr_catalog")
    p.add_argument("--schema", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--title", required=True)
    p.add_argument("--intro", default="")
    a = p.parse_args()

    lines = [f"# {a.title}", "", a.intro, "",
             f"Target: `{a.catalog}.{a.schema}` · captured {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC "
             f"on SQL warehouse `{WAREHOUSE_ID}`", ""]
    for title, note, sql in parse_blocks(open(a.sql_file).read()):
        sql = sql.format(catalog=a.catalog, schema=a.schema)
        r = run(sql)
        state = r["status"]["state"]
        print(f"{state:9} {title}")
        lines += [f"## {title}", ""] + ([note, ""] if note else []) + ["```sql", sql, "```", ""]
        if state != "SUCCEEDED":
            lines += [f"**{state}**: {r['status'].get('error', {}).get('message', '')[:500]}", ""]
            continue
        cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
        rows = r.get("result", {}).get("data_array", []) or []
        lines += [f"Statement `{r['statement_id']}` · {len(rows)} row(s)", "",
                  "| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
        lines += ["| " + " | ".join(fmt(v, c) for v, c in zip(row, cols)) + " |" for row in rows[:40]]
        lines += [""]
    open(a.out, "w").write("\n".join(lines))
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
