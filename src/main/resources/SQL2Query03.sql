-- Query result:
-- For each item of the types "STANDARD BURNISHED NICKEL", "ECONOMY ANODIZED STEEL" and "PROMO PLATED TIN",
-- output the supplier from Europe that has the lowest supply cost for the item.
-- The supply cost of an item is stored in the PartSupp table.
-- Arrange the result in descending order by the partkey and ascending by the supplier's name.
--
-- Result scheme:
-- [PARTKEY (↓1) | SUPPLYCOST | SUPPLIER (↑2)]
--
-- Points:
-- 0.5
--

SELECT P.PARTKEY as PARTKEY, PS.SUPPLYCOST as SUPPLYCOST, S.NAME as SUPPLIER
FROM PART P
         JOIN PARTSUPP PS ON PS.PARTKEY = P.PARTKEY
         JOIN SUPPLIER S ON S.SUPPKEY = PS.SUPPKEY
         JOIN NATION N ON N.NATIONKEY = S.NATIONKEY
WHERE P.TYPE IN ('STANDARD BURNISHED NICKEL', 'ECONOMY ANODIZED STEEL', 'PROMO PLATED TIN')
  AND N.REGIONKEY = 3
  AND PS.SUPPLYCOST = (SELECT MIN(PS2.SUPPLYCOST)
                       FROM PARTSUPP PS2
                                JOIN SUPPLIER S2 ON S2.SUPPKEY = PS2.SUPPKEY
                                JOIN NATION N2 ON N2.NATIONKEY = S2.NATIONKEY
                       WHERE PS2.PARTKEY = P.PARTKEY
                         AND N2.REGIONKEY = 3)
ORDER BY P.PARTKEY DESC, S.NAME ASC;
