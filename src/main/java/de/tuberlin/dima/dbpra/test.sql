SELECT Title, Price, StockQuantity
FROM Books
WHERE StockQuantity > 5 AND Price > 20
ORDER BY Price DESC;

SELECT b.Title, a.FirstName || ' ' || a.Lastname AS AuthorName, b.PublicationDate
FROM Books b
JOIN BookAuthors ba ON ba.BookID = b.BookID
JOIN Authors a ON a.AuthorID = ba.AuthorID
WHERE YEAR(b.PublicationDate) = 2023
ORDER BY b.Title ASC;

SELECT b.Category, COUNT(b.BookID) AS NumberOfBooks, ROUND(AVG(r.Rating),2) AS AverageRating
FROM Books b
JOIN Revies r  on b.BookID = r.BookID
GROUP BY b.Category
HAVING AVG(r.Rating) > 4
ORDER BY NumberOfBooks DESC;
FETCH FIRST 3 ROWS ONLY;

With pro_authors AS (SELECT AuthorID
                     FROM BookAuthors
                     GROUP BY AuthorID
                     HAVING COUNT(BookID) > 5)

SELECT
    CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
    COUNT(DISTINCT o.OrderID) AS BooksBought,
    SUM(od.PriceAtTime  * od.Quantity) AS TotalSpent
    STRING_AGG(b.Title, ', ') AS BooksList
FROM Customer
JOIN Orders o ON o.CustomerID = Customer.CustomerID
JOIN OrderDetails od ON od.OrderID = o.OrderID
JOIN Books b ON b.BookID = oi.BookID
JOIN BookAuthors ba ON ba.BookID = b.BookID
JOIN pro_authors pa ON pa.AuthorID = ba.AuthorID
WHERE EXTRACT(YEAR FROM o.OrderDate) = 2023 AND o.Status = 'Completed'
GROUP BY