-- Query result:
-- Output the name and account balance of all suppliers where the account balance is between 300 and 500.
-- Arrange the result in descending order according to the account balance.
-- <p/>
-- Result schema:
-- [NAME | ACCTBAL (↓)]
-- <p/>
-- Points:
-- 0.125
SELECT NAME, ACCTBAL FROM SUPPLIER WHERE ACCTBAL BETWEEN 300 AND 500 ORDER BY ACCTBAL DESC;
