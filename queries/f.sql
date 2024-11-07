WITH agg AS (
    SELECT
        ticker,
        SUM(open),
        MIN(close)
    FROM min_aggs
    GROUP BY ticker
)

SELECT 
    *,
    open + high + low + close as val
FROM min_aggs 
INNER JOIN agg ON min_aggs.ticker = agg.ticker
WHERE min_aggs.ticker = 'AAPL' 
AND min_aggs.window_start >= '2023-01-15' 
ORDER BY min_aggs.transactions
