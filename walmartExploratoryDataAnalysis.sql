SELECT *
FROM walmart_staging;

-- Here I want to examine several different factors before I begin dasboarding, mainly spanning 3 categories:

-- 1. Sales and Store Performance
-- 2. External Factors
-- 3. Forecasting

-- 1. Sales and Store Performance

-- Weekly sales by store and date to see trends over time
SELECT 
    Store, 
    ConvertedDate, 
    SUM(Weekly_Sales) AS Total_Weekly_Sales,
    SUM(SUM(Weekly_Sales)) OVER (PARTITION BY Store ORDER BY ConvertedDate) AS Running_Total_Weekly_Sales
FROM walmart_staging
GROUP BY Store, ConvertedDate
ORDER BY Store, ConvertedDate;

-- Store with the highest/lowest average weekly sales
-- This will help identify the best/worst performing stores
SELECT 
    Store, 
    AVG(Weekly_Sales) AS Avg_Weekly_Sales,
    RANK() OVER (ORDER BY AVG(Weekly_Sales) DESC) AS Sales_Rank
FROM walmart_staging
GROUP BY Store
ORDER BY Avg_Weekly_Sales DESC;

-- Store performance on holidays: This examines how stores perform during holiday weeks
SELECT 
    Holiday_Flagged, 
    SUM(Weekly_Sales) AS Total_Weekly_Sales
FROM walmart_staging
GROUP BY Holiday_Flagged;

-- Which years did Walmart have the highest sales overall
-- Aggregate sales by year to identify trends
SELECT 
    Year, 
    ROUND(SUM(Weekly_Sales), 2) AS Sales_for_Year
FROM walmart_staging
GROUP BY Year
ORDER BY Year;

-- 2. External Factors

-- Relationship between fuel prices and sales: Look at how fuel prices impact sales
SELECT 
    Year, 
    AVG(Weekly_Sales) AS Avg_Weekly_Sales, 
    AVG(Fuel_Price) AS Avg_Fuel_Price,
    AVG(Weekly_Sales) OVER (PARTITION BY Year ORDER BY Year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Moving_Avg_Sales
FROM walmart_staging
GROUP BY Year
ORDER BY Year;

-- Average temperature vs. total weekly sales by store
-- Analyze how weather conditions affect sales

SELECT 
    Store, 
    AVG(Temperature) AS Avg_Temperature, 
    SUM(Weekly_Sales) AS Total_Weekly_Sales,
    AVG(SUM(Weekly_Sales)) OVER (PARTITION BY Store ORDER BY AVG(Temperature) ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Moving_Avg_Sales
FROM walmart_staging
GROUP BY Store
ORDER BY Avg_Temperature DESC;

-- Impact of unemployment on sales over time
-- Shows how the economic factor of unemployment rates impacts Walmart's sales
SELECT 
    Year, 
    Month, 
    AVG(Weekly_Sales) AS Avg_Weekly_Sales, 
    AVG(Unemployment) AS Avg_Unemployment,
    SUM(AVG(Weekly_Sales)) OVER (PARTITION BY Year ORDER BY Month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Cumulative_Sales
FROM walmart_staging
GROUP BY Year, Month
ORDER BY Year, Month;

-- 3. Forecasting

-- Seasonal sales trends for forecasting: Analyzing time series sales trends
SELECT 
    ConvertedDate, 
    SUM(Weekly_Sales) AS Total_Weekly_Sales,
    AVG(SUM(Weekly_Sales)) OVER (ORDER BY ConvertedDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS Moving_Avg_Sales
FROM walmart_staging
GROUP BY ConvertedDate
ORDER BY ConvertedDate;

-- -------
-- Additional Queries for Deeper Insights:

-- 1. Sales performance during extreme temperature conditions (above 85°F or below 32°F)
SELECT 
    Store, 
    AVG(Temperature) AS Avg_Temperature, 
    SUM(Weekly_Sales) AS Total_Weekly_Sales,
    RANK() OVER (ORDER BY SUM(Weekly_Sales) DESC) AS Performance_Rank
FROM walmart_staging
WHERE Temperature > 85 OR Temperature < 32
GROUP BY Store
ORDER BY Avg_Temperature DESC;

-- 2. Investigating holiday sales decline: Compare average sales during holidays and non-holidays
-- Calculate average weekly sales by holiday flag
SELECT 
	Holiday_Flagged, 
	AVG(Weekly_Sales) AS Avg_Weekly_Sales
FROM walmart_staging
GROUP BY Holiday_Flagged;

-- 3. Correlation between CPI and sales: Determine how CPI impacts sales
-- Calculate average weekly sales and CPI by year
WITH AvgSales AS (
    SELECT 
        Year, 
        AVG(Weekly_Sales) AS Avg_Weekly_Sales,
        AVG(CPI) AS Avg_CPI
    FROM walmart_staging
    GROUP BY Year
)

-- Display the results with moving average sales
SELECT 
    Year, 
    Avg_CPI,
    Avg_Weekly_Sales,
    AVG(Avg_Weekly_Sales) OVER (ORDER BY Year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Moving_Avg_Sales
FROM AvgSales
ORDER BY Year;


-- 4. Sales trends across different months for forecasting seasonal patterns
SELECT 
    Month, 
    SUM(Weekly_Sales) AS Total_Weekly_Sales,
    RANK() OVER (ORDER BY SUM(Weekly_Sales) DESC) AS Monthly_Sales_Rank
FROM walmart_staging
GROUP BY Month
ORDER BY Month;


