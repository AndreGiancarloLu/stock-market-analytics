{{ config(
    materialized='table'
) }}
WITH stocks_enriched AS (
    SELECT *
    FROM {{ref('int_stocks_enriched')}}
),

stocks_daily_returns AS (
    SELECT 
        *,
        {{get_daily_return('open', 'close')}} AS daily_return
    FROM stocks_enriched
),

stocks_volatility AS (
    SELECT
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(YEAR FROM date) AS year,
        gics_sector,
        STDDEV(daily_return) AS volatility
    FROM
        stocks_daily_returns
    GROUP BY EXTRACT(MONTH FROM date), EXTRACT(YEAR FROM date), gics_sector
)

SELECT * FROM stocks_volatility