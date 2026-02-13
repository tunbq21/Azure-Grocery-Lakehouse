SELECT 
    ci.CityName,
    COUNT(DISTINCT s.SalesID) AS Number_of_Transactions,
    SUM(s.TotalPrice) AS City_Revenue
FROM sales s
JOIN customers cust ON s.CustomerID = cust.CustomerID
JOIN cities ci ON cust.CityID = ci.CityID
GROUP BY ci.CityName
ORDER BY City_Revenue DESC;