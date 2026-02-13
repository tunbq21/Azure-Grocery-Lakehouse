SELECT 
    e.FirstName || ' ' || e.LastName AS Employee_Name,
    COUNT(s.SalesID) AS Total_Orders,
    SUM(s.TotalPrice) AS Total_Sales_Value
FROM sales s
JOIN employees e ON s.SalesPersonID = e.EmployeeID
GROUP BY e.EmployeeID, e.FirstName, e.LastName
ORDER BY Total_Sales_Value DESC;