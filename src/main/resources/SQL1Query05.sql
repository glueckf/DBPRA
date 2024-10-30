-- Query result:
-- Output the number of customers and the average account balance of all customers who are in the 'BUILDING' market segment.
-- <p/>
-- Result schema:
-- [AMOUNT | AVERAGE]
-- <p/>
-- Points:
-- 0.125
SELECT COUNT(C.CUSTKEY) AS AMOUNT, AVG(C.ACCTBAL) AS AVERAGE
FROM CUSTOMER C
WHERE C.MKTSEGMENT = 'BUILDING';