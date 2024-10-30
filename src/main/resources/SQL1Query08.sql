-- Query result:
-- Output the amount, shipping instructions, shipping mode, return flag of all lineitems.
-- Group tuples based on shipping instructions, shipping mode, and return flag.
-- Only output results where the number is greater than 4999.
-- Arrange the result in descending order by amount.
-- <p/>
-- Result schema:
-- [AMOUNT (↓)| SHIPINSTRUCT | SHIPMODE| RETURNFLAG]
-- <p/>
-- Points:
-- 0.25
SELECT
    COUNT(LI.ORDERKEY) AS AMOUNT,
    LI.SHIPINSTRUCT,
    LI.SHIPMODE,
    LI.RETURNFLAG
FROM LINEITEM LI
GROUP BY LI.SHIPINSTRUCT, LI.SHIPMODE, LI.RETURNFLAG
HAVING COUNT(LI.ORDERKEY) > 4999
ORDER BY AMOUNT DESC;