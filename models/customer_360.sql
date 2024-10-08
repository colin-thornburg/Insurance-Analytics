-- models/integrated_views/customer_360.sql
{{ config(materialized='table') }}

WITH rps_policies AS (
    SELECT * FROM {{ ref('Insurance_RPS', 'policies') }}
),
claims_data AS (
    SELECT * FROM {{ ref('claims', 'claims') }}
),
risk_solutions AS (
    SELECT * FROM {{ ref('insurance_alternative_investments', 'risk_solutions') }}
)

SELECT
    rp.customer_id,
    COUNT(DISTINCT rp.policy_id) AS total_policies,
    SUM(rp.premium) AS total_premium,
    COUNT(DISTINCT c.claim_id) AS total_claims,
    SUM(c.claim_amount) AS total_claim_amount,
    COUNT(DISTINCT rs.solution_id) AS total_risk_solutions,
    SUM(rs.risk_amount) AS total_risk_exposure
FROM rps_policies rp
LEFT JOIN claims_data c ON rp.policy_id = c.policy_id
LEFT JOIN risk_solutions rs ON rp.customer_id = rs.customer_id
GROUP BY rp.customer_id