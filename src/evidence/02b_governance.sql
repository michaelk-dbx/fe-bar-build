-- name: PII column masks in action (query run by a user outside the SIU group)
SELECT is_account_group_member('mkr_siu') AS caller_in_siu_group, policyholder_id, full_name, email, phone, street_address, state, birth_year
FROM {catalog}.{schema}.silver_policyholders ORDER BY policyholder_id LIMIT 3;

-- name: Column masks and classification tags in Unity Catalog
SELECT 'column_mask' AS kind, table_name, column_name, mask_name AS value
FROM {catalog}.information_schema.column_masks WHERE table_schema = '{schema}'
UNION ALL
SELECT 'column_tag', table_name, column_name, tag_name
FROM {catalog}.information_schema.column_tags WHERE schema_name = '{schema}'
ORDER BY kind, table_name, column_name;

-- name: Table tags in Unity Catalog
SELECT table_name, concat_ws(', ', collect_list(concat(tag_name, '=', tag_value))) AS tags
FROM {catalog}.information_schema.table_tags WHERE schema_name = '{schema}'
GROUP BY table_name ORDER BY table_name;
