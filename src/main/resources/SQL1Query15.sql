-- Query result:
-- Find customers from America (with customer number, name, phone number and their nation) and "loss" they generated
-- from returned items.
-- Only list the top 10 customers who generated the most loss.
-- Note 1: Return status 'R' means returned.
-- Note 2: The loss here refers to the return of these items, i.e. lineitem's price (excluding tax, discount, or amount).
-- <p/>
-- Result schema:
-- [CUSTKEY | NATION | NAME | PHONE | LOSS (↓)]
-- <p/>
-- Points:
-- 0.625
SELECT C.CUSTKEY, N.NAME AS NATION, C.NAME, C.PHONE, SUM(LI.EXTENDEDPRICE) AS LOSS
FROM CUSTOMER C
JOIN NATION N ON C.NATIONKEY = N.NATIONKEY
JOIN REGION R ON N.REGIONKEY = R.REGIONKEY
JOIN ORDERS O ON C.CUSTKEY = O.CUSTKEY
JOIN LINEITEM LI ON O.ORDERKEY = LI.ORDERKEY
WHERE R.REGIONKEY = 1 AND LI.RETURNFLAG = 'R'
GROUP BY C.PHONE, C.NAME, C.CUSTKEY, N.NAME
ORDER BY LOSS DESC
FETCH FIRST 10 ROWS ONLY;