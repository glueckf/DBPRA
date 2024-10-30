-- Query result::
-- Output the amount, minimum, maximum, average and total price of all lineitems.
-- Group the aggregations by the shipping mode and arrange the result in ascending alphabetical order by shipping mode.
-- <p/>
-- Result schema:
-- [SHIPMODE (↑) | AMOUNT | MIN | MAX | AVG | TOTAL]
-- <p/>
-- Points:
-- 0.25
SELECT
    LI.SHIPMODE,
    COUNT(LI.ORDERKEY) AS AMOUNT,
    MIN(LI.EXTENDEDPRICE) AS MIN,
    MAX(LI.EXTENDEDPRICE) AS MAX,
    AVG(LI.EXTENDEDPRICE) AS AVG,
    SUM(LI.EXTENDEDPRICE) AS TOTAL
FROM LINEITEM LI
GROUP BY LI.SHIPMODE
ORDER BY LI.SHIPMODE ASC;