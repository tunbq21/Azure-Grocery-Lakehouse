 SELECT 
    c.CategoryName,
    SUM(s.Quantity) AS Total_Quantity,
    SUM(s.TotalPrice) AS Total_Revenue
FROM sales s
JOIN products p ON s.ProductID = p.ProductID
JOIN categories c ON p.CategoryID = c.CategoryID
GROUP BY c.CategoryName
ORDER BY Total_Revenue DESC;