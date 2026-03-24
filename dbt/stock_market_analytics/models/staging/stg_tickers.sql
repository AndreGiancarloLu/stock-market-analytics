WITH tickers AS (
    SELECT
        Symbol AS symbol,
        Security AS security,
        `GICS Sector` AS gics_sector,
        `GICS Sub-Industry` AS gics_sub_industry
    FROM {{ source('raw_data', 'sp500_tickers') }}
)

SELECT * FROM tickers