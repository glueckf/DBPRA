-- Query result:
-- For all brands and manufacturers , output the items that are in the lowest 1,5%
-- of the brand's price range (closed interval).
-- The price range is the interval between the cheapest and most expensive item.
-- Arrange the results in ascending order by manufacturer and brand, and then in descending order by name.
--
-- Result scheme:
-- [MFGR (↑1) | BRAND (↑2) | NAME (↓3)]
--
-- Points:
-- 0.5
--
-- Zuerst berechnen wir die Preisgrenzen für alle MFGR-BRAND Kombinationen
WITH price_boundaries AS (
    SELECT MFGR,
           BRAND,
           MIN(RETAILPRICE) as min_price,
           MAX(RETAILPRICE) as max_price
    FROM PART
    GROUP BY MFGR, BRAND
)
-- Dann joinen wir diese mit der ursprünglichen Tabelle
SELECT p.MFGR, p.BRAND, p.NAME
FROM PART p
         JOIN price_boundaries pb
              ON p.MFGR = pb.MFGR
                  AND p.BRAND = pb.BRAND
WHERE p.RETAILPRICE <= pb.min_price + 0.015 * (pb.max_price - pb.min_price)
ORDER BY p.MFGR ASC, p.BRAND ASC, p.NAME DESC;