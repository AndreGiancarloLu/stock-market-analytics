WITH stocks AS (SELECT
    CAST(date AS DATE) AS date,
    CAST(symbol AS STRING) AS symbol,
    CAST(open AS FLOAT64) AS open,
    CAST(high AS FLOAT64) AS high,
    CAST(low AS FLOAT64) AS low,
    CAST(close AS FLOAT64) AS close,
    CAST(volume AS INT64) AS volume
FROM 
    {{ source('raw_data', 'stock_prices') }}
)

SELECT * FROM stocks