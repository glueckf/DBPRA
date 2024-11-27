CREATE OR REPLACE PROCEDURE ComputeAffiliationsOfBars()
    BEGIN
    DELETE FROM BARS_WITH_AFFILIATIONS;
        FOR b AS (SELECT
                            BIU.id AS ID,
                            BIU.name AS NAME,
                            PIU.name AS PLACENAME
                        FROM BARS_IN_UK BIU
                                 CROSS JOIN PLACES_IN_UK PIU
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM PLACES_IN_UK PIU2
                            WHERE ROUND4(HAVERSINE(BIU.longitude, BIU.latitude, PIU2.longitude, PIU2.latitude)) <
                                  ROUND4(HAVERSINE(BIU.longitude, BIU.latitude, PIU.longitude, PIU.latitude))
                        )
                        ORDER BY BIU.id)
            DO
                INSERT INTO BARS_WITH_AFFILIATIONS(ID, BARNAME, PLACENAME)
                VALUES (b.ID, b.NAME, b.PLACENAME);

        END FOR;
    END;