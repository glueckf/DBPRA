-- Query result:
-- Output all suppliers who carry at least one item (part) from the manufacturer “Manufacturer#2”
-- or the brand “Brand#52”.
-- Arrange the result in descending order by name.
--
-- Result schema:
-- [NAME (↓) | ADDRESS | PHONE]
--
-- Points:
-- 0.25
--
SELECT DISTINCT S.NAME as NAME, S.ADDRESS as ADDRESS, S.PHONE as PHONE
FROM SUPPLIER S
JOIN PARTSUPP PS ON PS.SUPPKEY = S.SUPPKEY
JOIN PART P ON P.PARTKEY = PS.PARTKEY
WHERE P.MFGR = 'Manufacturer#2' OR P.BRAND = 'Brand#52'
ORDER BY NAME DESC;