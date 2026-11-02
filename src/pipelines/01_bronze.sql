-- BRONZE: source-system exports ingested as-is from the UC landing volume.
-- Streaming tables over read_files (Auto Loader) pick up only new files on each run,
-- so a new monthly FNOL export is processed incrementally rather than re-reading history.

CREATE OR REFRESH STREAMING TABLE bronze_claims_fnol
COMMENT 'Raw first-notice-of-loss claim submissions (monthly JSONL exports from the claims system), including resubmissions and malformed records'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing_path}/claims_fnol/',
  format => 'json',
  schemaHints => 'claim_id STRING, policy_id STRING, contractor_id STRING, claimed_amount DOUBLE, public_adjuster BOOLEAN, fnol_version INT'
);

CREATE OR REFRESH STREAMING TABLE bronze_policyholders
COMMENT 'Raw policyholder export from the policy admin system (contains PII)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/policy_admin/', format => 'csv', header => true,
  pathGlobFilter => 'policyholders.csv', schemaHints => 'zip_code STRING, phone STRING');

CREATE OR REFRESH STREAMING TABLE bronze_policies
COMMENT 'Raw homeowners policy export from the policy admin system'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/policy_admin/', format => 'csv', header => true,
  pathGlobFilter => 'policies.csv', schemaHints => 'zip_code STRING, coverage_increase_date STRING');

CREATE OR REFRESH STREAMING TABLE bronze_contractors
COMMENT 'Raw contractor/vendor master export'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/vendors/', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze_weather_events
COMMENT 'Raw NOAA-style storm events feed: one row per event with a comma-separated list of affected ZIP3 areas'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/weather/', format => 'csv', header => true,
  schemaHints => 'affected_zip3 STRING');

CREATE OR REFRESH STREAMING TABLE bronze_claim_outcomes
COMMENT 'Raw SIU referral and investigation outcomes for closed claims (legacy rules-based process)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/siu/', format => 'csv', header => true);
