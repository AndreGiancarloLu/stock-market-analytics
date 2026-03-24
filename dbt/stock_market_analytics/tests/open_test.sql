SELECT * FROM {{ ref('int_stocks_enriched') }}
WHERE open > high OR open < low