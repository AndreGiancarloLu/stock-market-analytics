SELECT * FROM {{ ref('int_stocks_enriched') }}
WHERE 
    volume < 0 OR open < 0 OR close < 0 OR high < 0 OR low < 0