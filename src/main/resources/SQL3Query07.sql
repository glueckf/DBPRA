-- Task 7:
--AND REGEXP_LIKE(SUBSTR(TwitterId, 2), '^[a-z0-9+-]+$')
--
/*
ALTER TABLE CustomerContactData
    ADD CONSTRAINT Twitter_CHECK
        CHECK(
            SUBSTR(TwitterID, 1, 1) = '@'
                AND (
                LENGTH(SUBSTR(TwitterId, 2)) <= 22
                    AND TRANSLATE(SUBSTR(TwitterId, 2),
                                  'abcdefghijklmnopqrstuvwxyz0123456789*-',
                                  'abcdefghijklmnopqrstuvwxyz0123456789*-') =
                        SUBSTR(TwitterId, 2)
                )
            );
*/
-- Points:
-- 1
--
ALTER TABLE CustomerContactData
    ADD CONSTRAINT TwitterID_CHECK
        CHECK(
            TwitterID IS NULL
                OR (
                SUBSTR(TwitterID, 1, 1) = '@'
                    AND LENGTH(SUBSTR(TwitterId, 2)) <= 22
                    AND LOWER(SUBSTR(TwitterId, 2)) = SUBSTR(TwitterId, 2)
                    AND TRANSLATE(SUBSTR(TwitterId, 2),'', 'abcdefghijklmnopqrstuvwxyz0123456789*-') = ''
                )
            );