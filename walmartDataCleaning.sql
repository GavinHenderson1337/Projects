SELECT *
FROM walmart;
-- Check data types 
SHOW COLUMNS 
FROM walmart;
-- Steps for Cleaning Data:
-- 1. Remove Duplicates
-- 2. Null Values or Blank Values
-- 3. Remove/Add Any Columns


-- 1. Remove Duplicates

-- Create a staging table to prevent damage to raw data

CREATE TABLE walmart_staging
LIKE walmart;

SELECT *
FROM walmart_staging;

INSERT walmart_staging	-- Imports the data from walmart into the staging table
SELECT *
FROM walmart;

SELECT *
FROM walmart_staging;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Store, Date, Weekly_Sales, Holiday_Flag, Temperature, Fuel_Price, CPI, Unemployment)
FROM walmart_staging;

WITH duplicate_cte AS(  -- This CTE exists to be able to conduct testing on this Query
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Store, Date, Weekly_Sales, Holiday_Flag, Temperature, Fuel_Price, CPI, Unemployment) AS row_num
FROM walmart_staging
)

SELECT *		-- Test for Duplicates (There are none). Therefore there is no reason to delete any.
FROM duplicate_cte
WHERE row_num > 1;



-- 2. Check for Null, Blank, or missing values
SELECT 				-- Checks if stores are missing
DISTINCT Store
FROM walmart_staging
ORDER BY store;

-- Checks for nulls in dataset(there are none)
SELECT * 
FROM walmart_staging 
WHERE coalesce(Store, Date, Weekly_Sales, Holiday_Flag, Temperature, Fuel_Price, CPI, Unemployment) IS NULL;

-- 3. Remove/Add Any Columns

SELECT  -- Trying to Assess how to categorize fuel_price to make a high, medium, and low category
Fuel_Price,
AVG(Fuel_Price) OVER()
FROM walmart_staging;

ALTER TABLE walmart_staging
ADD COLUMN Fuel_Price_Buckets VARCHAR(50);

SELECT * 
FROM walmart_staging;


UPDATE walmart_staging			-- Making buckets for fuel prices
SET Fuel_Price_Buckets = CASE
    WHEN Fuel_Price BETWEEN 0 AND 2.75 THEN 'Extremely Inexpensive'
    WHEN Fuel_Price BETWEEN 2.75 AND 3.10 THEN 'Relatively Inexpensive'
    WHEN Fuel_Price BETWEEN 3.10 AND 3.5 THEN 'Average'
    WHEN Fuel_Price BETWEEN 3.5 AND 4 THEN 'Expensive'
    ELSE 'Expensive'
END;

SELECT * 
FROM walmart_staging;

ALTER TABLE walmart_staging			-- Adding columns for dates so that we can have more specific analysis
ADD COLUMN ConvertedDate DATE,
ADD COLUMN Week INT,
ADD COLUMN Month INT,
ADD COLUMN Year INT;

SELECT * 
FROM walmart_staging;


SELECT 											
    STR_TO_DATE(Date, '%d-%m-%Y') AS ConvertedDate,
    WEEK( STR_TO_DATE(Date, '%d-%m-%Y')) AS Week,
    MONTH(STR_TO_DATE(Date, '%d-%m-%Y')) AS Month,
    YEAR(STR_TO_DATE(Date, '%d-%m-%Y')) AS Year
FROM walmart_staging;

UPDATE walmart_staging		-- The goal here is to standardize the DateTime so that it is easier to perform analysis on
SET 
    ConvertedDate = STR_TO_DATE(Date, '%d-%m-%Y'),
    Week = WEEK(STR_TO_DATE(Date, '%d-%m-%Y')),
    Month = MONTH(STR_TO_DATE(Date, '%d-%m-%Y')),
    Year = YEAR(STR_TO_DATE(Date, '%d-%m-%Y'));


SELECT *
FROM walmart_staging;

ALTER TABLE walmart_staging
DROP COLUMN Date;

SELECT *
FROM walmart_staging;

SELECT
Holiday_Flag,
CASE 
	WHEN Holiday_Flag = 1 THEN 'Holiday'
    ELSE 'No Holiday'
END AS Holiday_Flagged
FROM walmart_staging;

ALTER TABLE walmart_staging
ADD COLUMN Holiday_Flagged VARCHAR(20);

UPDATE walmart_staging
SET Holiday_Flagged = CASE 
    WHEN Holiday_Flag = 1 THEN 'Holiday'
    ELSE 'No Holiday'
END;

ALTER TABLE walmart_staging
DROP COLUMN Holiday_Flag;

SELECT *
FROM walmart_staging;








