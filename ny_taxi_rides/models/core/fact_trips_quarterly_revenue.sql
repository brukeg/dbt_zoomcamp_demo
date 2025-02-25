{{
    config(
        materialized='table'
    )
}}

WITH quarterly_revenue AS (
    SELECT
        service_type,
        year,
        quarter,
        year_quarter,
        SUM(total_amount) AS quarter_revenue
    FROM {{ ref('fact_trips') }}
    WHERE year IN (2019,2020)
    GROUP BY service_type, year, quarter, year_quarter
),
past_revenue AS (
SELECT 
    quarterly_revenue.service_type,
    quarterly_revenue.year,
    quarterly_revenue.quarter,
    quarterly_revenue.year_quarter,
    quarterly_revenue.quarter_revenue,
    LAG(quarterly_revenue.quarter_revenue) OVER (
        PARTITION BY quarterly_revenue.service_type, quarterly_revenue.quarter 
        ORDER BY year
    ) AS prev_quarter_revenue
FROM quarterly_revenue
)
SELECT
    past_revenue.service_type,
    past_revenue.year,
    past_revenue.quarter,
    past_revenue.year_quarter,
    past_revenue.quarter_revenue,
    past_revenue.prev_quarter_revenue,
    CASE 
        WHEN past_revenue.prev_quarter_revenue IS NOT NULL 
        THEN (past_revenue.quarter_revenue - past_revenue.prev_quarter_revenue) / past_revenue.prev_quarter_revenue * 100
        ELSE NULL
    END AS yoy_growth
FROM past_revenue
