-- Query result:
-- Output the total volume (sum over price in lineitems) of all customers who are in market segment 'AUTOMOBILE' or 'MACHINERY'.
-- Group the output based on customer's key and name.
-- Arrange the result in ascending order by total volume.
-- <p/>
-- Result schema:
-- [NAME | TOTAL_VOLUME (↑)]
-- <p/>
-- Points:
-- 0.5
SELECT C.NAME AS NAME, SUM(LI.EXTENDEDPRICE) AS TOTAL_VOLUME
FROM CUSTOMER AS C
JOIN ORDERS AS O ON C.CUSTKEY = O.CUSTKEY
JOIN LINEITEM AS LI ON O.ORDERKEY = LI.ORDERKEY
WHERE MKTSEGMENT = 'AUTOMOBILE' OR MKTSEGMENT = 'MACHINERY'
GROUP BY C.CUSTKEY, C.NAME
ORDER BY TOTAL_VOLUME ASC;