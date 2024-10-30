-- Query result:
-- For each possible combination of shipping mode and return flag, find the average and the maximum processing time (between receipt and shipment of the lineitem.
-- Sort the results ascending by the shipping mode and descending by return flag.
-- Note: To calculate date differences you can use DB2's days() function.
-- Example: days(laterDate) - days(earlierDate) = Elapsed days from earlierDate to laterDate.
-- <p/>
-- Result schema:
-- [SHIPMODE (↑1) | RETURNFLAG (↓2) | AVG_TIME | MAX_TIME]
-- <p/>
-- Points:
-- 0.375
SELECT SHIPMODE, RETURNFLAG, AVG(DAYS(RECEIPTDATE) - DAYS(SHIPDATE)) AS AVG_TIME, MAX(DAYS(RECEIPTDATE)-DAYS(SHIPDATE)) AS MAX_TIME
FROM LINEITEM
GROUP BY SHIPMODE, RETURNFLAG
ORDER BY SHIPMODE ASC, RETURNFLAG DESC;