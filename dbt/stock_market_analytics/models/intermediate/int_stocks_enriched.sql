WITH stocks AS (
    SELECT *
    FROM {{ref('stg_stocks')}}
),

tickers AS (
    SELECT *
    FROM {{ref('stg_tickers')}}
),

enriched AS (
    SELECT 
        s.date,
        s.symbol,
        s.open,
        s.high,
        s.low,
        s.close,
        s.volume,
        t.security,
        t.gics_sector,
        t.gics_sub_industry
    FROM stocks s
    LEFT JOIN tickers t
    ON s.symbol = t.symbol
)

SELECT * FROM enriched