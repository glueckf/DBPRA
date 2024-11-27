CREATE VIEW DISTANCE_PLACES AS (
    SELECT ID,
           NAME,
           LONGITUDE,
           LATITUDE,
           ROUND4(HAVERSINE(LONGITUDE,
                            LATITUDE,
                            (SELECT LONGITUDE FROM PLACES_IN_UK WHERE NAME = 'London'),
                            (SELECT LATITUDE FROM PLACES_IN_UK WHERE NAME = 'London')
                  )) AS DISTANCE_LONDON
    FROM PLACES_IN_UK
    WHERE Name != 'London'
)