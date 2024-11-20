-- Setting the trigger

CREATE OR REPLACE TRIGGER PhoneNumberTrigger
    AFTER UPDATE OF Phone ON CustomerContactData
    REFERENCING NEW AS newP OLD AS oldP
    FOR EACH ROW
    WHEN (newP.Phone <> oldP.Phone)
BEGIN
        IF NOT EXISTS (SELECT 1 FROM PhoneChanges)
        OR CURRENT_TIMESTAMP > (SELECT CHANGEDATE
                                  FROM PhoneChanges
                                  ORDER BY CHANGEDATE
                                  FETCH FIRST ROW ONLY) + 15 SECONDS
        THEN
            INSERT INTO PhoneChanges (CHANGEDATE, CUSTKEY, OLDPHONE)
            VALUES (CURRENT_TIMESTAMP, oldP.Custkey, oldP.Phone);
        ELSE
            SIGNAL SQLSTATE '70001';
        END IF;
END;
