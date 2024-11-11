-- Query result:
-- Determine the brand(s) offered by most suppliers.
-- Arrange the result in ascending order by brand.
--
-- Result scheme:
-- [Marke (↑)]
--
-- Points:
-- 0.5
--
WITH supplier_counts AS (
    SELECT
        P.BRAND,
        COUNT(DISTINCT S.SUPPKEY) as supplier_count
    FROM PART P
             JOIN PARTSUPP PS ON PS.PARTKEY = P.PARTKEY
             JOIN SUPPLIER S ON S.SUPPKEY = PS.SUPPKEY
    GROUP BY P.BRAND
)
SELECT BRAND
FROM supplier_counts
WHERE supplier_count = (
    SELECT MAX(supplier_count)
    FROM supplier_counts
)
ORDER BY BRAND ASC;


