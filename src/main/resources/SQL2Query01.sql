-- Query result:
-- Output all market segments whose average account balance is below the average
-- account balance of all customers.
-- Arrange the results in ascending order by market segment.

-- Result schema:
-- [MKTSEGMENT (↑) | ACCTBAL]
--
-- Points:
-- 0.25
--
SELECT MKTSEGMENT, AVG(ACCTBAL) AS BALANCE
FROM CUSTOMER
GROUP BY MKTSEGMENT
HAVING AVG(ACCTBAL) < (SELECT AVG(ACCTBAL) FROM CUSTOMER)
ORDER BY MKTSEGMENT ASC;

