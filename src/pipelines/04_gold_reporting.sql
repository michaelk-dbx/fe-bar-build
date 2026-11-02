-- GOLD reporting: scorecards and KPIs built on gold_claim_features (Genie and the app).

-- Contractor scorecard: where does fraud concentrate?
CREATE OR REFRESH MATERIALIZED VIEW gold_contractor_risk
COMMENT 'Per-contractor claim volume, severity, storm-mismatch share and SIU-confirmed fraud: used to spot contractor fraud rings'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  f.contractor_id, k.contractor_name, k.state, k.years_in_business,
  count(*)                                                      AS claims,
  count(DISTINCT f.policyholder_id)                             AS distinct_policyholders,
  round(avg(f.claimed_amount), 2)                               AS avg_claimed_amount,
  round(avg(f.amount_vs_peril_median), 3)                       AS avg_amount_vs_peril_median,
  round(avg(CASE WHEN f.is_weather_peril THEN CAST(NOT f.storm_matched AS INT) END), 3) AS storm_mismatch_rate,
  count_if(f.siu_referred)                                      AS siu_referrals,
  count_if(f.label_confirmed_fraud = 1)                         AS confirmed_fraud_claims,
  round(count_if(f.label_confirmed_fraud = 1) / nullif(count_if(f.claim_status = 'closed'), 0), 3) AS confirmed_fraud_rate
FROM gold_claim_features f
JOIN silver_contractors k ON k.contractor_id = f.contractor_id
GROUP BY ALL;

-- Monthly claims KPIs for claims leadership (Genie and the app's dashboard view).
CREATE OR REFRESH MATERIALIZED VIEW gold_claims_kpis_monthly
COMMENT 'Monthly claims KPIs by state and peril: volume, claimed and paid USD, SIU referrals, confirmed fraud, SIU hit rate, reporting lag'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  report_month, state, peril,
  count(*)                                                      AS claims,
  round(sum(claimed_amount), 2)                                 AS claimed_usd,
  round(sum(paid_amount), 2)                                    AS paid_usd,
  count_if(siu_referred)                                        AS siu_referrals,
  count_if(label_confirmed_fraud = 1)                           AS confirmed_fraud_claims,
  round(count_if(label_confirmed_fraud = 1) / nullif(count_if(siu_referred), 0), 3) AS siu_hit_rate,
  round(avg(report_lag_days), 1)                                AS avg_report_lag_days,
  count_if(claim_status = 'open')                               AS open_claims
FROM gold_claim_features
GROUP BY ALL;
