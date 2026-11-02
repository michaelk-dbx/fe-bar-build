-- GOLD: business-ready tables for the fraud model, Lakebase serving, Genie and the app.

-- One row per claim with fraud-signal features. Every history-based feature is point-in-time:
-- it only uses claims and SIU outcomes known *before* this claim was reported, so the model
-- never learns from the future.
CREATE OR REFRESH MATERIALIZED VIEW gold_claim_features (
  claim_id                         STRING  NOT NULL COMMENT 'Claim identifier',
  policy_id                        STRING  COMMENT 'Policy the claim was filed against',
  policyholder_id                  STRING  COMMENT 'Policyholder who filed the claim',
  state                            STRING  COMMENT 'US state of the insured property',
  zip3                             STRING  COMMENT 'First 3 digits of the property ZIP code',
  product                          STRING  COMMENT 'Policy form: HO-3 (standard homeowners), HO-5 (premium), DP-3 (rental dwelling)',
  peril                            STRING  COMMENT 'Cause of loss: water, fire, theft, hail, wind, freeze',
  is_weather_peril                 BOOLEAN COMMENT 'True for hail, wind and freeze claims, which should match a recorded storm',
  loss_date                        DATE    COMMENT 'Date the loss occurred',
  report_date                      DATE    COMMENT 'Date the claim was first reported (FNOL)',
  report_month                     DATE    COMMENT 'First day of the report month',
  claimed_amount                   DOUBLE  COMMENT 'Amount claimed in USD',
  dwelling_coverage                DOUBLE  COMMENT 'Dwelling coverage limit in USD',
  deductible                       INT     COMMENT 'Policy deductible in USD',
  square_feet                      INT     COMMENT 'Home size in square feet',
  year_built                       INT     COMMENT 'Year the home was built',
  roof_age_years                   INT     COMMENT 'Roof age in years',
  days_since_policy_start          INT     COMMENT 'Days from policy effective date to loss date (fraud signal: very new policies)',
  days_since_coverage_increase     INT     COMMENT 'Days from the latest coverage increase to loss date; NULL if coverage was never increased',
  report_lag_days                  INT     COMMENT 'Days between loss and report (fraud signal: late reporting)',
  amount_to_coverage_ratio         DOUBLE  COMMENT 'Claimed amount divided by dwelling coverage',
  amount_per_sqft                  DOUBLE  COMMENT 'Claimed amount per square foot of the home',
  amount_vs_peril_median           DOUBLE  COMMENT 'Claimed amount divided by the median claim for the same peril (fraud signal: inflation)',
  storm_matched                    BOOLEAN COMMENT 'For weather perils: was a storm recorded in this ZIP3 within 3 days of the loss? NULL for non-weather perils',
  matched_storm_severity           INT     COMMENT 'Highest severity (1-5) of a matching storm, if any',
  contractor_id                    STRING  COMMENT 'Contractor that submitted the repair estimate, if any',
  contractor_prior_claims          BIGINT  COMMENT 'Claims this contractor worked on before this claim was reported',
  contractor_prior_confirmed_fraud BIGINT  COMMENT 'Claims by this contractor confirmed as fraud by SIU before this claim was reported',
  policy_prior_claims_3y           BIGINT  COMMENT 'Claims on the same policy in the 3 years before this claim was reported',
  public_adjuster                  BOOLEAN COMMENT 'True if the policyholder hired a public adjuster',
  description                      STRING  COMMENT 'Adjuster / FNOL narrative of the loss',
  claim_status                     STRING  COMMENT 'open (no outcome yet) or closed',
  siu_referred                     BOOLEAN COMMENT 'Was the closed claim referred to the Special Investigations Unit (SIU) under the legacy process?',
  siu_outcome                      STRING  COMMENT 'SIU result: confirmed_fraud, cleared, or NULL if never investigated',
  paid_amount                      DOUBLE  COMMENT 'Amount paid on the closed claim in USD',
  label_confirmed_fraud            INT     COMMENT 'Training label: 1 = SIU confirmed fraud, 0 = closed without confirmed fraud, NULL = still open',
  CONSTRAINT policy_found EXPECT (policy_id IS NOT NULL AND dwelling_coverage IS NOT NULL)
)
COMMENT 'One row per claim with point-in-time fraud features and SIU labels: the fraud model training and scoring table'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH claims AS (
  SELECT c.*, p.policyholder_id, p.state, p.zip3, p.product, p.effective_date, p.coverage_increase_date,
         p.dwelling_coverage, p.deductible, p.square_feet, p.year_built, p.roof_age_years
  FROM silver_claims c
  LEFT JOIN silver_policies p ON p.policy_id = c.policy_id
),
peril_median AS (
  SELECT peril, percentile_approx(claimed_amount, 0.5) AS median_amount FROM silver_claims GROUP BY peril
),
storm AS (
  SELECT c.claim_id, max(w.severity) AS matched_storm_severity
  FROM claims c
  JOIN silver_weather_event_areas w
    ON w.state = c.state AND w.zip3 = c.zip3
   AND c.loss_date BETWEEN date_sub(w.begin_date, 3) AND date_add(w.end_date, 3)
   AND (w.peril = c.peril OR (c.peril IN ('hail', 'wind') AND w.peril IN ('hail', 'wind')))
  GROUP BY c.claim_id
),
contractor_history AS (
  SELECT c.claim_id,
         count(h.claim_id) AS contractor_prior_claims,
         count_if(o.siu_outcome = 'confirmed_fraud' AND o.closed_date < c.report_date) AS contractor_prior_confirmed_fraud
  FROM claims c
  JOIN silver_claims h ON h.contractor_id = c.contractor_id AND h.report_date < c.report_date
  LEFT JOIN silver_claim_outcomes o ON o.claim_id = h.claim_id
  GROUP BY c.claim_id
),
policy_history AS (
  SELECT c.claim_id, count(h.claim_id) AS policy_prior_claims_3y
  FROM claims c
  JOIN silver_claims h ON h.policy_id = c.policy_id
   AND h.report_date < c.report_date AND h.report_date >= add_months(c.report_date, -36)
  GROUP BY c.claim_id
)
SELECT
  c.claim_id, c.policy_id, c.policyholder_id, c.state, c.zip3, c.product, c.peril,
  c.peril IN ('hail', 'wind', 'freeze')                         AS is_weather_peril,
  c.loss_date, c.report_date, CAST(date_trunc('MONTH', c.report_date) AS DATE) AS report_month,
  c.claimed_amount, c.dwelling_coverage, c.deductible, c.square_feet, c.year_built, c.roof_age_years,
  datediff(c.loss_date, c.effective_date)                       AS days_since_policy_start,
  datediff(c.loss_date, c.coverage_increase_date)               AS days_since_coverage_increase,
  datediff(c.report_date, c.loss_date)                          AS report_lag_days,
  round(c.claimed_amount / c.dwelling_coverage, 4)              AS amount_to_coverage_ratio,
  round(c.claimed_amount / c.square_feet, 2)                    AS amount_per_sqft,
  round(c.claimed_amount / m.median_amount, 3)                  AS amount_vs_peril_median,
  CASE WHEN c.peril IN ('hail', 'wind', 'freeze') THEN s.claim_id IS NOT NULL END AS storm_matched,
  CAST(s.matched_storm_severity AS INT)                         AS matched_storm_severity,
  c.contractor_id,
  coalesce(ch.contractor_prior_claims, 0)                       AS contractor_prior_claims,
  coalesce(ch.contractor_prior_confirmed_fraud, 0)              AS contractor_prior_confirmed_fraud,
  coalesce(ph.policy_prior_claims_3y, 0)                        AS policy_prior_claims_3y,
  c.public_adjuster, c.description,
  CASE WHEN o.claim_id IS NULL THEN 'open' ELSE 'closed' END    AS claim_status,
  o.siu_referred, o.siu_outcome, o.paid_amount,
  CASE WHEN o.claim_id IS NULL THEN NULL
       WHEN o.siu_outcome = 'confirmed_fraud' THEN 1 ELSE 0 END AS label_confirmed_fraud
FROM claims c
JOIN peril_median m ON m.peril = c.peril
LEFT JOIN storm s ON s.claim_id = c.claim_id
LEFT JOIN contractor_history ch ON ch.claim_id = c.claim_id
LEFT JOIN policy_history ph ON ph.claim_id = c.claim_id
LEFT JOIN silver_claim_outcomes o ON o.claim_id = c.claim_id;
