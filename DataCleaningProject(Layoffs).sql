-- Data Cleaning Project

USE world_layoffs;

SELECT * 
FROM world_layoffs.layoffs AS layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns

-- Staging( to avoid messing up raw database)
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging; 


INSERT layoffs_staging -- What this function does is it creates a duplicate table, thererfore not altering the data in the original database
SELECT * 
FROM layoffs;


SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, `date`)  AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS(  -- This CTE exists to be able to conduct testing on this Query
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions)  AS row_num
FROM layoffs_staging
) 
SELECT *
FROM duplicate_cte
WHERE row_num >1;

SELECT * -- This was a test used to ensure that the "duplicates" were duplicates
FROM layoffs_staging
WHERE company = 'Casper';





WITH duplicate_cte AS(  -- This CTE exists to be able to conduct testing on this Query
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date`, 
stage, country, funds_raised_millions)  AS row_num
FROM layoffs_staging
) 
DELETE 				-- This deletes duplicates( ERROR)
FROM duplicate_cte
WHERE row_num > 1;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *   -- IDentifies Duplicates
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date`, 
stage, country, funds_raised_millions)  AS row_num
FROM layoffs_staging;

/*
DELETE -- Deletes the duplicate values
FROM layoffs_staging2
WHERE row_num > 1; 
*/

SELECT *  
FROM layoffs_staging2;





-- Standardizing data

SELECT company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2;

UPDATE layoffs_staging2 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%' ;


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT `date`
FROM layoffs_staging2;
 
 -- USE FOR NULLS------------------------------------------------------------------------------------------------------
UPDATE layoffs_staging2
SET `date` = NULL
WHERE `date` = 'NULL';

UPDATE layoffs_staging2
SET `date` = CASE WHEN `date` IS NOT NULL THEN STR_TO_DATE(`date`, '%m/%d/%Y') ELSE NULL END;
--------------------------
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE industry = "NULL"
OR industry = ''
;

SELECT * -- Not able to populate because there was no reference data
FROM layoffs_staging2
WHERE company LIKE  "Bally%";

SELECT * 
FROM layoffs_staging2
WHERE company =  "Airbnb";


UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry= '' OR industry = "NULL";

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry = "NULL"  OR t1.industry = '' OR t1.industry IS NULL)
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1. industry = t2.industry 
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;


SELECT total_laid_off, percentage_laid_off
FROM layoffs_staging2
WHERE total_laid_off = "NULL" OR total_laid_off = ""
OR percentage_laid_off = "NULL"  OR percentage_laid_off = "";

UPDATE layoffs_staging2      -- Updating values to NULL 
SET total_laid_off = NULL 
WHERE total_laid_off= '' OR total_laid_off = "NULL";

UPDATE layoffs_staging2
SET percentage_laid_off = NULL 
WHERE percentage_laid_off= '' OR percentage_laid_off = "NULL";


SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE -- Since we have no way to extract this data, and we are going to use it for analysis, we are going to delete this
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2 -- No use for this column anymore
DROP COLUMN row_num;











