UPDATE PARTSUPP
SET AVAILQTY = AVAILQTY - ?
WHERE PARTKEY = ? AND SUPPKEY = ?
