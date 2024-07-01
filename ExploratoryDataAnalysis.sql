-- Exploratory Data Analysis
SELECT * 
FROM layoffs_staging2;


SELECT * 
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT * -- Companies that laid off everyone
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC; -- Checks size of Company vs percentage laid off

SELECT * -- Companies that laid off everyone
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY  funds_raised_millions DESC; -- Checks funding of Company vs percentage laid off


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT* 
FROM layoffs_staging2;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


SELECT  stage, SUM(total_laid_off) -- Which stage of company gets laid off the most
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;



SELECT  company, SUM(total_laid_off) -- Which company is laying off the most
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC

)

SELECT `MONTH`, 
total_off,
SUM(total_off)OVER(ORDER BY  `MONTH`) AS rolling_total  -- Added Rolling total
FROM Rolling_Total;


SELECT  company, SUM(total_laid_off) -- Which company is laying off the most
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT  company,YEAR(`date`), SUM(total_laid_off) -- Which company is laying off the most
FROM layoffs_staging2
GROUP BY company, `date`
ORDER BY 3 DESC;



WITH Company_Year ( company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank  AS(

SELECT * , 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY Ranking ASC

)
SELECT * -- Largest 5 companies to lay people off
FROM Company_Year_Rank
WHERE Ranking <= 5;
