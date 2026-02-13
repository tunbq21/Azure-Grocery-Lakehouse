CREATE VIEW Gold_Top_Employees AS
SELECT *
FROM OPENROWSET(
    BULK 'https://grocerysaleaccount.blob.core.windows.net/gold/top_employees/',
    FORMAT = 'PARQUET'
) AS [result]