# Step 2b: Unity Catalog governance: PII masks and classification tags

Column masks (`mask_pii`, `mask_phone`, created by `src/notebooks/00_governance_setup.py`) are attached in the pipeline definition of `silver_policyholders`; tags are applied by `src/notebooks/02_governance_apply.py`. PII uses Databricks' built-in `class.*` classification tags; layer and PII flags use the metastore's governed `medallion_layer` / `contains_pii` tags.

Target: `serverless_stable_mkr_catalog.mkr_claims` · captured 2026-11-02 15:12 UTC on SQL warehouse `22d236430dbb5df0`

## PII column masks in action (query run by a user outside the SIU group)

```sql
SELECT is_account_group_member('mkr_siu') AS caller_in_siu_group, policyholder_id, full_name, email, phone, street_address, state, birth_year
FROM serverless_stable_mkr_catalog.mkr_claims.silver_policyholders ORDER BY policyholder_id LIMIT 3
```

Statement `01f1d6cf-df07-1a7c-b55c-f9a6ef3e9917` · 3 row(s)

| caller_in_siu_group | policyholder_id | full_name | email | phone | street_address | state | birth_year |
|---|---|---|---|---|---|---|---|
| false | PH000001 | Donald Walker | ***REDACTED*** | ***-***-3890 | ***REDACTED*** | LA | 1961 |
| false | PH000002 | Phillip Garcia | ***REDACTED*** | ***-***-1849 | ***REDACTED*** | OK | 1950 |
| false | PH000003 | Derek Zuniga | ***REDACTED*** | ***-***-3503 | ***REDACTED*** | KS | 1949 |

## Column masks and classification tags in Unity Catalog

```sql
SELECT 'column_mask' AS kind, table_name, column_name, mask_name AS value
FROM serverless_stable_mkr_catalog.information_schema.column_masks WHERE table_schema = 'mkr_claims'
UNION ALL
SELECT 'column_tag', table_name, column_name, tag_name
FROM serverless_stable_mkr_catalog.information_schema.column_tags WHERE schema_name = 'mkr_claims'
ORDER BY kind, table_name, column_name
```

Statement `01f1d6cf-e017-1ba9-9557-4385a370fe11` · 12 row(s)

| kind | table_name | column_name | value |
|---|---|---|---|
| column_mask | silver_policyholders | email | serverless_stable_mkr_catalog.mkr_claims.mask_pii |
| column_mask | silver_policyholders | phone | serverless_stable_mkr_catalog.mkr_claims.mask_phone |
| column_mask | silver_policyholders | street_address | serverless_stable_mkr_catalog.mkr_claims.mask_pii |
| column_tag | bronze_policyholders | date_of_birth | class.date_of_birth |
| column_tag | bronze_policyholders | email | class.email_address |
| column_tag | bronze_policyholders | full_name | class.name |
| column_tag | bronze_policyholders | phone | class.phone_number |
| column_tag | bronze_policyholders | street_address | class.location |
| column_tag | silver_policyholders | email | class.email_address |
| column_tag | silver_policyholders | full_name | class.name |
| column_tag | silver_policyholders | phone | class.phone_number |
| column_tag | silver_policyholders | street_address | class.location |

## Table tags in Unity Catalog

```sql
SELECT table_name, concat_ws(', ', collect_list(concat(tag_name, '=', tag_value))) AS tags
FROM serverless_stable_mkr_catalog.information_schema.table_tags WHERE schema_name = 'mkr_claims'
GROUP BY table_name ORDER BY table_name
```

Statement `01f1d6cf-e17e-1743-8452-03472636977f` · 16 row(s)

| table_name | tags |
|---|---|
| bronze_claim_outcomes | contains_pii=false, medallion_layer=bronze, mkr_owner_team=claims_data |
| bronze_claims_fnol | contains_pii=false, medallion_layer=bronze, mkr_owner_team=claims_data |
| bronze_contractors | mkr_owner_team=claims_data, contains_pii=false, medallion_layer=bronze |
| bronze_policies | mkr_owner_team=claims_data, contains_pii=false, medallion_layer=bronze |
| bronze_policyholders | mkr_owner_team=claims_data, contains_pii=true, medallion_layer=bronze |
| bronze_weather_events | medallion_layer=bronze, contains_pii=false, mkr_owner_team=claims_data |
| gold_claim_features | mkr_owner_team=claims_data, contains_pii=false, medallion_layer=gold |
| gold_claims_kpis_monthly | contains_pii=false, medallion_layer=gold, mkr_owner_team=claims_data |
| gold_contractor_risk | medallion_layer=gold, mkr_owner_team=claims_data, contains_pii=false |
| silver_claim_outcomes | mkr_owner_team=claims_data, medallion_layer=silver, contains_pii=false |
| silver_claims | medallion_layer=silver, mkr_owner_team=claims_data, contains_pii=false |
| silver_claims_validated | medallion_layer=silver, contains_pii=false, mkr_owner_team=claims_data |
| silver_contractors | mkr_owner_team=claims_data, contains_pii=false, medallion_layer=silver |
| silver_policies | medallion_layer=silver, mkr_owner_team=claims_data, contains_pii=false |
| silver_policyholders | contains_pii=true, medallion_layer=silver, mkr_owner_team=claims_data |
| silver_weather_event_areas | contains_pii=false, mkr_owner_team=claims_data, medallion_layer=silver |
