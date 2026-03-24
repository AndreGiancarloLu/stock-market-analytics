SELECT * FROM {{ ref('int_stocks_enriched') }}
WHERE close > high OR close < low