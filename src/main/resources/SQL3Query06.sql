-- Task 6:
--
-- Points:
-- 1
--
ALTER TABLE CustomerContactData
    ADD CONSTRAINT CHECK_Phone
        CHECK (
            SUBSTR(Phone, 1, 3) IN ('+45', '+46', '+47')
            AND SUBSTR(Phone, 4, 1) = ' '
            AND (CAST(SUBSTR(Phone, 5, 3) AS INT) BETWEEN 147 AND 158
                OR CAST(SUBSTR(Phone, 5,3) AS INT) BETWEEN 171 AND 192)
            AND SUBSTR(Phone, 8 ,1) = ' '
            AND LENGTH(SUBSTR(Phone, 9)) BETWEEN 8 AND 10
            );

