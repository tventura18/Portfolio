#Exploratory World Life Expectancy 
USE `world_life_expectancy`;

#For each Country, this calculates:
#Maximum life expectancy (MAX)
#Minimum life expectancy (MIN)
#Difference between max and min rounded to 1 decimal (Life_Increase_15_Years)
#HAVING filters out countries where life expectancy is zero at either end.
#ORDER BY Life_Increase_15_Years DESC sorts countries by the largest gain first.
#Useful for seeing which countries improved the most over the years.
SELECT MAX(year)
FROM world_life_expectancy;

SELECT MIN(year)
FROM world_life_expectancy;

SELECT Country,
MAX(`Life expectancy`),
MIN(`Life expectancy`),
ROUND(MAX(`Life expectancy`)-MIN(`Life expectancy`), 1) as Life_Increase_15_Years
FROM world_life_expectancy
GROUP BY Country
HAVING MAX(`Life expectancy`)<> 0
AND MIN(`Life expectancy`) <> 0
ORDER BY Life_Increase_15_Years DESC
;


/***Testing MAX verses MIN values for different countries ***/
SELECT Country,
Year,
`Life expectancy` as Life_Expectancy_Max,
MAX(`Life expectancy`) OVER(PARTITION BY Country) as MAX_Life_expectancy
#ROUND(MAX(`Life expectancy`)-MIN(`Life expectancy`), 1) as Life_Increase_15_Yearsworld_life_expectancy
FROM world_life_expectancy
#GROUP BY Country, Year
#HAVING MAX(`Life expectancy`)<> 0
#AND MIN(`Life expectancy`) <> 0
WHERE Country = 'Haiti'
ORDER BY Country, Year;

/*** Finding Life expectancy from 2007-2022***/
WITH Ordered AS (
    SELECT
        Country,
        Year,
        `Life expectancy`,
        FIRST_VALUE(Year) OVER (PARTITION BY Country ORDER BY Year ASC) AS Year_Start,
        FIRST_VALUE(Year) OVER (PARTITION BY Country ORDER BY Year DESC) AS Year_End,
        FIRST_VALUE(`Life expectancy`) OVER (PARTITION BY Country ORDER BY Year ASC) AS Start_Expectancy,
        FIRST_VALUE(`Life expectancy`) OVER (PARTITION BY Country ORDER BY Year DESC) AS End_Expectancy
    FROM world_life_expectancy
)
SELECT DISTINCT
    Country,
    Year_Start,
    Start_Expectancy,
    Year_End,
    End_Expectancy,
    ROUND(End_Expectancy - Start_Expectancy, 1) AS Life_Expectancy_Change,
    CASE 
        WHEN End_Expectancy > Start_Expectancy THEN '+ Increase'
        WHEN End_Expectancy < Start_Expectancy THEN '- Decrease'
        ELSE 'No Change'
    END AS Trend
FROM Ordered
ORDER BY Life_Expectancy_Change DESC;

 SELECT
        Country,
        Year,
        `Life expectancy`,
        FIRST_VALUE(Year) OVER (PARTITION BY Country ORDER BY Year ASC) AS Year_Start,
        FIRST_VALUE(Year) OVER (PARTITION BY Country ORDER BY Year DESC) AS Year_End,
        FIRST_VALUE(`Life expectancy`) OVER (PARTITION BY Country ORDER BY Year ASC) AS Start_Expectancy,
        FIRST_VALUE(`Life expectancy`) OVER (PARTITION BY Country ORDER BY Year DESC) AS End_Expectancy
    FROM world_life_expectancy
    WHERE `Life expectancy` <= 0  -- exclude invalid or placeholder data;

WITH Ordered AS (
    SELECT
        Country,
        Year,
        `Life expectancy`,
        FIRST_VALUE(Year) OVER (PARTITION BY Country ORDER BY Year ASC) AS Year_Start,
        FIRST_VALUE(Year) OVER (PARTITION BY Country ORDER BY Year DESC) AS Year_End,
        FIRST_VALUE(`Life expectancy`) OVER (PARTITION BY Country ORDER BY Year ASC) AS Start_Expectancy,
        FIRST_VALUE(`Life expectancy`) OVER (PARTITION BY Country ORDER BY Year DESC) AS End_Expectancy
    FROM world_life_expectancy
    WHERE `Life expectancy` > 0  -- exclude invalid or placeholder data
)
SELECT DISTINCT
    Country,
    Year_Start,
    Start_Expectancy,
    Year_End,
    End_Expectancy,
    ROUND(End_Expectancy - Start_Expectancy, 1) AS Life_Expectancy_Change,
    CASE 
        WHEN End_Expectancy > Start_Expectancy THEN '+ Increase'
        WHEN End_Expectancy < Start_Expectancy THEN '- Decrease'
        ELSE 'No Change'
    END AS Trend
FROM Ordered
ORDER BY Life_Expectancy_Change DESC;

#Computes the average life expectancy for each Year, ignoring zero values.
#Rounded to 1 decimal.
#Orders results chronologically.
#Useful for seeing which countries improved the most over the years.
SELECT Year, 
ROUND(AVG(`Life expectancy`),1)
FROM world_life_expectancy
WHERE `Life expectancy` <> 0
GROUP BY Year
ORDER BY Year
;

#Calculates each country’s average life expectancy and average GDP, ignoring zero averages.
#ORDER BY Average_GDP DESC lists richest countries first.
#Helps explore GDP-life expectancy relationships.
SELECT
	Country,
    ROUND(AVG(`Life expectancy`), 1) AS Average_Life_Expectancy,
    ROUND(AVG(GDP), 1) AS Average_GDP
FROM world_life_expectancy
GROUP BY Country
HAVING Average_Life_Expectancy > 0
AND Average_GDP > 0
#ORDER BY Average_Life_Expectancy ASC
##ORDER BY Average_GDP ASC
ORDER BY Average_GDP DESC
;

#Splits dataset into “high GDP” (≥1500) and “low GDP” (≤1500).
#Counts the number of records in each group.
#Computes average life expectancy for each group.
#Quick check of whether higher GDP countries have higher life expectancy.
SELECT
	SUM(CASE WHEN GDP >= 1500 THEN 1 ELSE 0 END) AS Higher_GDP,
    AVG(CASE WHEN GDP >= 1500 THEN `Life expectancy` ELSE NULL END) AS High_Life_expectancy,
    SUM(CASE WHEN GDP <= 1500 THEN 1 ELSE 0 END) AS Lower_GDP,
    AVG(CASE WHEN GDP <= 1500 THEN `Life expectancy` ELSE NULL END) AS Low_Life_expectancy
FROM world_life_expectancy
;

#Returns all 2938 rows and all columns.
#Useful for general inspection.
SELECT *
FROM world_life_expectancy;

#Groups countries by Status (likely “Developed”, “Developing”, etc.)
#Computes average life expectancy for each group.
#Useful for comparing life expectancy by country development status.
SELECT 
	Status,
    ROUND(AVG(`Life expectancy`), 1) as avg_life_expectancy
FROM world_life_expectancy
GROUP BY Status;

#Similar to previous query but also counts number of countries per status.
SELECT 
	Status,
    COUNT(DISTINCT Country) AS Total_countries,
    ROUND(AVG(`Life expectancy`), 1) as avg_life_expectancy
FROM world_life_expectancy
GROUP BY Status;

#Computes average life expectancy and BMI per country.
#Filters out invalid values.
#Orders by BMI ascending.
SELECT
	Country,
    ROUND(AVG(`Life expectancy`), 1) AS Average_Life_Expectancy,
    ROUND(AVG(BMI), 1) AS Average_BMI
FROM world_life_expectancy
GROUP BY Country
HAVING Average_Life_Expectancy > 0
AND Average_BMI > 0
#ORDER BY Average_Life_Expectancy ASC
##ORDER BY Average_GDP ASC
#ORDER BY Average_BMI DESC
ORDER BY Average_BMI ASC
;

#Adds a rolling sum of Adult Mortality per country over years.
#PARTITION BY Country resets sum for each country.
#Useful to see cumulative mortality trends.
SELECT
	Country,
    `Life expectancy`,
    `Adult Mortality`,
    SUM(`Adult Mortality`) OVER(PARTITION BY Country ORDER BY Year) AS ROLLING_TOTAL
FROM world_life_expectancy
;


SELECT *
FROM world_life_expectancy;
#2938 rows returned

#Finds min and max infant deaths per country per year.
SELECT MIN(`infant deaths`),
MAX(`infant deaths`),
Country, Year
FROM world_life_expectancy
GROUP BY Country, Year
;


SELECT Country, 
Year,
`infant deaths`
FROM world_life_expectancy
Where Country = 'Albania';

#Shows records with zero infant deaths.
SELECT Country, 
Year,
`infant deaths`
FROM world_life_expectancy
Where `infant deaths` = 0
;


SELECT Country, 
Year
FROM world_life_expectancy
WHERE `infant deaths` = 0
;

SELECT DISTINCT Year
FROM world_life_expectancy;
#Years 2007-2022

SELECT Country,
Year, 
`infant deaths`,
`under-five deaths`
FROM world_life_expectancy
WHERE `infant deaths` = 0
;
#NOTHING WAS RETURNED

SELECT * 
FROM world_life_expectancy
;

#checking for null values
SELECT Country, 
Year, 
`Life expectancy`,
`percentage expenditure` 
FROM world_life_expectancy
WHERE `percentage expenditure` IS NULL 
;
## NOTHING WAS RETURNED

#Returns 2007–2022.
Count all records:
SELECT COUNT(*)
FROM world_life_expectancy
;
##2938 total records from 2008-2022

#Counts records with zero infant deaths (848 records).
SELECT COUNT(*)
FROM world_life_expectancy
WHERE `infant deaths` = 0
;
#848 records

SELECT COUNT(DISTINCT Year)
FROM world_life_expectancy
WHERE `infant deaths` = 0
; 
# 16 years

SELECT COUNT(DISTINCT Country)
FROM world_life_expectancy;
## 193 Countries

#Counts countries with at least one year of zero infant deaths (69 countries).
SELECT COUNT(distinct Country)
FROM world_life_expectancy
WHERE `infant deaths` = 0
; 
#69 countries

#Counts  zero infant deaths per country.

SELECT Country, 
#COUNT(Country)
COUNT(`infant deaths`)
FROM world_life_expectancy
WHERE `infant deaths` = 0
group by Country
; 
#73 countries

SELECT Country,
COUNT(`infant deaths`)
FROM world_life_expectancy
WHERE `infant deaths` = 0
GROUP BY COUNTRY
HAVING COUNT(Country) = 16
;
#Returns 72

SELECT user, host, plugin FROM mysql.user;
