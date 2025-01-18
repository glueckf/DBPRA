-- Query result:
-- Consider all groups of orders that have the same order date, the same clerk, and the same shipping priority.
-- From these groups, accept orders whose total price is lower than the total price of another order in this group.
-- Return the customers associated with these orders (excluding duplicates).
-- Sort the result by name in descending order.
-- <p/>
-- Result schema:
-- [NAME (↓)]
-- <p/>
-- Points:
-- 0.625
SELECT DISTINCT C.NAME
FROM ORDERS o1
    JOIN CUSTOMER AS C ON o1.CUSTKEY = C.CUSTKEY
WHERE EXISTS(
    SELECT *
    FROM ORDERS o2
    WHERE o1.ORDERDATE = o2.ORDERDATE
        AND o1.CLERK = o2.CLERK
        AND o1.SHIPPRIORITY = o2.SHIPPRIORITY
        AND o1.TOTALPRICE < o2.TOTALPRICE
)
ORDER BY C.NAME DESC;


