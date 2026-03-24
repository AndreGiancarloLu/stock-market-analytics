{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"},
    cluster_by=["gics_sector"]
) }}
WITH stocks_enriched AS (
    SELECT
        *
    FROM 
    {{ref('int_stocks_enriched')}}
),

stocks_daily_returns AS (
    SELECT 
        *,
        {{get_daily_return('open', 'close')}} AS daily_return
    FROM stocks_enriched
),

stocks_with_sector_returns AS (
    SELECT
        date,
        gics_sector,
        AVG(daily_return) AS avg_daily_return
    FROM stocks_daily_returns
    GROUP BY date, gics_sector
),

stocks_with_sector_return_cumulative AS (
    SELECT 
        *,
        SUM(avg_daily_return) OVER (PARTITION BY gics_sector ORDER BY date) AS cumulative_return
    FROM stocks_with_sector_returns
)

SELECT * FROM stocks_with_sector_return_cumulative