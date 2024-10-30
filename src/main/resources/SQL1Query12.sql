-- Query result:
-- Output the amount, region name, and shipping mode of all lineitems excluding order lines in Canada.
-- Group the result based on region name and shipping mode.
-- Another tip: It is not necessary to use the Customer table, it is enough to go over the Suppliers.
-- Order the result in ascending order by amount.
-- <p/>
-- Result schema:
-- [AMOUNT(↑) | NAME | SHIPMODE]
-- <p/>
-- Points:
-- 0.5
SELECT COUNT(LI.ORDERKEY) AS AMOUNT, R.NAME, LI.SHIPMODE
FROM LINEITEM AS LI,REGION AS R, NATION AS N, SUPPLIER AS S
WHERE LI.SUPPKEY = S.SUPPKEY AND S.NATIONKEY = N.NATIONKEY AND N.REGIONKEY = R.REGIONKEY AND N.NAME != 'CANADA'
GROUP BY R.NAME, LI.SHIPMODE
ORDER BY AMOUNT ASC;