-- Query result:
-- Output the amount, minimum, maximum and average price of all order lines.
-- <p/>
-- Result schema:
-- [AMOUNT | MIN_PRICE | MAX_PRICE | AVG_PRICE]
-- <p/>
-- Points:
-- 0.125
SELECT COUNT(LI.ORDERKEY) AS AMOUNT, MIN(LI.EXTENDEDPRICE) AS MIN_PRICE, MAX(LI.EXTENDEDPRICE) AS MAX_PRICE, AVG(LI.EXTENDEDPRICE) AS AVG_PRICE
FROM LINEITEM LI;
