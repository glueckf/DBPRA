-- Query result:
-- For each customer, calculate how many of their orders contained only
-- items from local suppliers.
-- Local supplier means that the supplier is located in the same country as the customer.
-- Return all customers who had at least one such local order.
-- Arrange the results in ascending order by region, country and descending by customer name.
--
-- Result scheme:
-- [CUSTOMER (↓3) | AMOUNT | NATION (↑2) | REGION (↑1)]
--
-- Points:
-- 0.75
--

WITH local_orders AS(
    SELECT O.ORDERKEY
    FROM ORDERS O
    JOIN CUSTOMER ON O.CUSTKEY = CUSTOMER.CUSTKEY
    JOIN LINEITEM ON O.ORDERKEY = LINEITEM.ORDERKEY
    JOIN SUPPLIER ON LINEITEM.SUPPKEY = SUPPLIER.SUPPKEY
    GROUP BY O.ORDERKEY, CUSTOMER.NATIONKEY
    HAVING MIN(SUPPLIER.NATIONKEY) = CUSTOMER.NATIONKEY
    AND MAX(SUPPLIER.NATIONKEY) = CUSTOMER.NATIONKEY
)

SELECT CUSTOMER.NAME AS CUSTOMER, COUNT(DISTINCT local_orders.ORDERKEY) AS AMOUNT, NATION.NAME AS NATION, REGION.NAME AS REGION
FROM CUSTOMER
JOIN ORDERS on CUSTOMER.CUSTKEY = ORDERS.CUSTKEY
JOIN NATION on CUSTOMER.NATIONKEY = NATION.NATIONKEY
JOIN REGION on NATION.REGIONKEY = REGION.REGIONKEY
JOIN local_orders on ORDERS.ORDERKEY = local_orders.ORDERKEY
GROUP BY CUSTOMER.NAME, NATION.NAME, REGION.NAME
ORDER BY REGION.NAME, NATION.NAME, CUSTOMER.NAME DESC;



