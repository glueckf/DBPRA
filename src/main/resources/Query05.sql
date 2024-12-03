-- Return the suppkey of a supplier that has the lowast price and an available quantity higher than ?

SELECT *
FROM PARTSUPP
WHERE PARTKEY = ?
AND AVAILQTY >= ?
ORDER BY SUPPLYCOST ASC
FETCH FIRST ROW ONLY;