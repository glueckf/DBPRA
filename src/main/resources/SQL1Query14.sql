-- Query result:
-- Output the customer numbers of all customers who come from Germany, are not in market segment 'FURNITURE'
-- and whose average order price is greater than 190500.
-- Each customer number should only be output once.
-- Arrange the result in descending order.
-- <p/>
-- Result schema:
-- [CUSTOMER (↓)]
-- <p/>
-- Points:
-- 0.625
SELECT DISTINCT C.CUSTKEY AS CUSTOMER
FROM CUSTOMER AS C
JOIN NATION AS N ON C.NATIONKEY = N.NATIONKEY
JOIN ORDERS AS O ON C.CUSTKEY = O.CUSTKEY
WHERE
    N.NAME = 'GERMANY'
    AND C.MKTSEGMENT != 'FURNITURE'
GROUP BY C.CUSTKEY
HAVING AVG(O.TOTALPRICE) > 190500
ORDER BY C.CUSTKEY DESC;