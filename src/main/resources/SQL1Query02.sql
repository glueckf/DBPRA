-- Query result:
-- Output all market segment of customers excluding duplicates in ascending alphabetical order.
-- <p/>
-- Result schema:
-- [MKTSEGMENT (↑)]
-- <p/>
-- Points:
-- 0.125
SELECT DISTINCT MKTSEGMENT FROM CUSTOMER ORDER BY MKTSEGMENT ASC;
