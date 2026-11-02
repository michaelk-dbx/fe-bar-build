-- SILVER: typed, validated, deduplicated and governed.

-- Claims, step 1: enforce data quality on every submission. Malformed records are dropped
-- and counted in the pipeline event log, so data quality is measured, not silently lost.
CREATE OR REFRESH STREAMING TABLE silver_claims_validated (
  CONSTRAINT has_claim_id        EXPECT (claim_id IS NOT NULL)                ON VIOLATION DROP ROW,
  CONSTRAINT has_policy_id       EXPECT (policy_id IS NOT NULL)               ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount     EXPECT (claimed_amount > 0)                  ON VIOLATION DROP ROW,
  CONSTRAINT reported_after_loss EXPECT (report_date >= loss_date)            ON VIOLATION DROP ROW,
  CONSTRAINT known_peril         EXPECT (peril IN ('water', 'fire', 'theft', 'hail', 'wind', 'freeze'))
)
COMMENT 'FNOL submissions that passed data quality rules (may still contain resubmissions)'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  claim_id, policy_id, peril,
  CAST(loss_date AS DATE)    AS loss_date,
  CAST(report_date AS DATE)  AS report_date,
  CAST(submitted_at AS DATE) AS submitted_at,
  claimed_amount, contractor_id, public_adjuster, description, fnol_version, _ingested_at
FROM STREAM(bronze_claims_fnol);

-- Claims, step 2: one current row per claim. AUTO CDC keeps the latest fnol_version,
-- so a resubmitted claim updates the record instead of being counted twice.
CREATE OR REFRESH STREAMING TABLE silver_claims
COMMENT 'One row per claim: latest valid FNOL submission (resubmissions collapsed by claim_id)'
TBLPROPERTIES ('quality' = 'silver');

CREATE FLOW silver_claims_dedupe AS AUTO CDC INTO silver_claims
FROM STREAM(silver_claims_validated)
KEYS (claim_id)
SEQUENCE BY fnol_version
STORED AS SCD TYPE 1;

-- Policyholders: PII is masked by Unity Catalog column masks for anyone outside the SIU group.
-- Date of birth is reduced to birth year (data minimisation: nothing downstream needs the full date).
CREATE OR REFRESH MATERIALIZED VIEW silver_policyholders (
  policyholder_id STRING NOT NULL COMMENT 'Policyholder identifier',
  full_name       STRING COMMENT 'Policyholder full name',
  email           STRING COMMENT 'Contact email (PII, masked outside SIU)'  MASK ${pii_mask_fn},
  phone           STRING COMMENT 'Contact phone (PII, last 4 digits shown outside SIU)' MASK ${phone_mask_fn},
  street_address  STRING COMMENT 'Street address (PII, masked outside SIU)' MASK ${pii_mask_fn},
  city            STRING COMMENT 'City of the insured property',
  state           STRING COMMENT 'US state of the insured property',
  zip_code        STRING COMMENT '5-digit ZIP code of the insured property',
  birth_year      INT    COMMENT 'Policyholder birth year (full date of birth is not retained)'
)
COMMENT 'Policyholders with PII governed by column masks'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT policyholder_id, full_name, email, phone, street_address, city, state, zip_code,
          year(CAST(date_of_birth AS DATE)) AS birth_year
FROM bronze_policyholders;

CREATE OR REFRESH MATERIALIZED VIEW silver_policies (
  CONSTRAINT valid_coverage EXPECT (dwelling_coverage > 0) ON VIOLATION DROP ROW
)
COMMENT 'Homeowners policies (HO-3, HO-5, DP-3) with typed dates and coverage terms'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  policy_id, policyholder_id, product, state, zip_code, left(zip_code, 3) AS zip3,
  CAST(effective_date AS DATE)         AS effective_date,
  CAST(coverage_increase_date AS DATE) AS coverage_increase_date,
  CAST(dwelling_coverage AS DOUBLE)    AS dwelling_coverage,
  CAST(deductible AS INT)              AS deductible,
  CAST(year_built AS INT)              AS year_built,
  CAST(roof_age_years AS INT)          AS roof_age_years,
  CAST(square_feet AS INT)             AS square_feet,
  CAST(annual_premium AS DOUBLE)       AS annual_premium
FROM bronze_policies;

CREATE OR REFRESH MATERIALIZED VIEW silver_contractors
COMMENT 'Contractors and restoration vendors who submit repair estimates on claims'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT contractor_id, contractor_name, state, license_number, CAST(years_in_business AS INT) AS years_in_business
FROM bronze_contractors;

-- Weather events exploded to one row per affected ZIP3, so claims can be matched by area and date.
CREATE OR REFRESH MATERIALIZED VIEW silver_weather_event_areas
COMMENT 'Storm events by affected ZIP3 area: used to verify that weather claims match a recorded storm'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT event_id, state, peril, CAST(severity AS INT) AS severity,
          CAST(begin_date AS DATE) AS begin_date, CAST(end_date AS DATE) AS end_date,
          zip3
FROM bronze_weather_events
LATERAL VIEW explode(split(affected_zip3, ',')) AS zip3;

CREATE OR REFRESH MATERIALIZED VIEW silver_claim_outcomes (
  CONSTRAINT outcome_requires_referral EXPECT (siu_outcome IS NULL OR siu_referred)
)
COMMENT 'Closed-claim outcomes from the legacy SIU process: referral, investigation result and amount paid'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT claim_id, CAST(closed_date AS DATE) AS closed_date,
          CAST(siu_referred AS BOOLEAN) AS siu_referred, siu_outcome,
          CAST(paid_amount AS DOUBLE) AS paid_amount
FROM bronze_claim_outcomes;
