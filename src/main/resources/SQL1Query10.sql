-- Query result:
-- Output the number of customers per nation.
-- Group output based on nation's name.
-- Arrange the result in ascending order of amount.
-- <p/>
-- Result schema:
-- [NATION | AMOUNT (↑)]
-- <p/>
-- Points:
-- 0.5
SELECT N.NAME AS NATION, COUNT(C.CUSTKEY) AS AMOUNT
FROM CUSTOMER AS C, NATION AS N
WHERE C.NATIONKEY = N.NATIONKEY
GROUP BY N.NAME
ORDER BY AMOUNT ASC;