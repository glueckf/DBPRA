-- Query result:
-- For all suppliers from Canada, output the items for which the available supply value
-- is less than 3/1000 of the available supply value of all items from this supplier.
-- The available supply value of an item is the product of the available quantity and the supply cost.
-- Arrange the result in descending order by value and suppkey.
--
-- Result schema:
-- [SUPPKEY (↓2) | PARTKEY | PART_NAME| VALUE (↓1)]
--
-- Points:
-- 0.75
--
WITH supplier_values AS(
    SELECT ps.SUPPKEY, SUM(ps.SUPPLYCOST * ps.AVAILQTY) AS TOTAL
    FROM PARTSUPP ps
    group by ps.SUPPKEY
)

SELECT PARTSUPP.SUPPKEY, PARTSUPP.PARTKEY, PART.NAME AS PART_NAME, PARTSUPP.SUPPLYCOST * PARTSUPP.AVAILQTY AS VALUE
FROM PARTSUPP
JOIN PART ON PARTSUPP.PARTKEY = PART.PARTKEY
JOIN SUPPLIER ON PARTSUPP.SUPPKEY = SUPPLIER.SUPPKEY
JOIN NATION ON SUPPLIER.NATIONKEY = NATION.NATIONKEY
JOIN supplier_values ON SUPPLIER.SUPPKEY = supplier_values.SUPPKEY
WHERE NATION.NAME = 'CANADA' AND (PARTSUPP.SUPPLYCOST * PARTSUPP.AVAILQTY) < 0.003 * supplier_values.TOTAL
ORDER BY VALUE DESC, PARTSUPP.SUPPKEY DESC;