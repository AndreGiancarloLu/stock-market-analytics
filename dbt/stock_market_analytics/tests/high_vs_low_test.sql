SELECT * FROM {{ref('int_stocks_enriched')}}
WHERE high < low