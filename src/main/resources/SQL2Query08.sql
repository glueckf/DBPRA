-- Query result:
-- For each shipping mode, find the name of the supplier  whose line items
-- were most often shipped using this shipping mode.
-- Arrange the result in descending order by number and ascending order by name.
--
-- Result schema:
-- [SHIPMODE | NAME(↑2) | AMOUNT(↓1)]
--
-- Points:
-- 0.5
--

WITH counts AS (
    SELECT LI.SHIPMODE AS SHIPMODE, S.NAME AS NAME, COUNT(*) AS AMOUNT
    FROM LINEITEM LI
             JOIN PARTSUPP PS ON PS.PARTKEY = LI.PARTKEY AND PS.SUPPKEY = LI.SUPPKEY
             JOIN SUPPLIER S ON S.SUPPKEY = PS.SUPPKEY
    GROUP BY LI.SHIPMODE, S.NAME
)
SELECT SHIPMODE, NAME, AMOUNT
FROM counts c1
WHERE (SHIPMODE, AMOUNT) IN (
    SELECT SHIPMODE, MAX(AMOUNT)
    FROM counts
    GROUP BY SHIPMODE
)
ORDER BY AMOUNT DESC, NAME ASC;