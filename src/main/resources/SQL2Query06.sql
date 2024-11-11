-- Query result:
-- Output the total account balances  and a number of all customers whose
-- account balance is more than 100.
-- Group them by a country (nation), and sort the results in descending order by its name.
--
-- Result schema:
-- [NAME (↓) | SUM | AMOUNT]
--
-- Punkte:
-- 0.5
--
SELECT N.NAME AS NAME, SUM(C.ACCTBAL) AS SUM, COUNT(C.CUSTKEY) AS AMOUNT
FROM CUSTOMER C
JOIN NATION N ON N.NATIONKEY = C.NATIONKEY
WHERE C.ACCTBAL > 100
GROUP BY N.NAME
ORDER BY N.NAME DESC;
