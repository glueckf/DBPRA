-- Query result:
-- Find orders that were placed between January 1st, 1995 and December 31st, 1997 (closed interval) and for which at least 2 line items
-- were delivered later than promised.
-- For each order priority output how many orders were affected.
-- Arrange the result in descending order by amount.
--
-- Result schema:
-- [ORDERPRIORITY| AMOUNT (↓)]
--
-- Points:
-- 0.5
--

WITH late_deliveries AS (
    SELECT L.ORDERKEY,
           COUNT(*) AS AMOUNT
    FROM LINEITEM L
    WHERE L.RECEIPTDATE > L.COMMITDATE
    GROUP BY L.ORDERKEY
    HAVING COUNT(*) >= 2
)

SELECT
    O.ORDERPRIORITY AS PRIORITY,
    COUNT(*) AS AMOUNT
FROM ORDERS O
JOIN late_deliveries LD ON  O.ORDERKEY = LD.ORDERKEY
WHERE O.ORDERDATE BETWEEN '1995-01-01' AND '1997-12-31'
GROUP BY O.ORDERPRIORITY
ORDER BY AMOUNT DESC;