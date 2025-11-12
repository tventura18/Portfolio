#Date cleaning for World Life Expectancy in My SQL
# Tells which db to use to query
USE `world_life_expectancy`;

#Turns off the “safe updates” mode in MySQL.
#Allows UPDATE or DELETE statements without specifying a WHERE clause that uses a key.
#Necessary when doing bulk updates or deletes.
SET SQL_SAFE_UPDATES = 0;

#Shows all rows and columns in the table.
#Useful for inspecting the data before cleaning.
select * 
from world_life_expectancy;

#Returns the total number of rows in the table.
#Helps you understand the dataset size before cleaning.
SELECT count(*)
from world_life_expectancy;

#Uses a window function to detect duplicates:
#ROW_NUMBER() assigns a sequential number for each Country + Year combination.
#ROW_NUM > 1 identifies all but the first occurrence, i.e., duplicates.
SELECT *
FROM(
	SELECT 
	Row_ID,
	CONCAT(Country, Year),
	ROW_NUMBER() OVER (PARTITION BY CONCAT(Country, Year) ORDER BY CONCAT(Country, Year)) AS ROW_NUM
	FROM world_life_expectancy
) AS ROW_TABLE
WHERE ROW_NUM > 1;

#Deletes all duplicates identified in the previous query.
#Keeps only the first row for each Country + Year.
DELETE FROM world_life_expectancy
WHERE 
Row_ID IN 
(
	SELECT Row_ID
	FROM(
		SELECT 
		Row_ID,
		CONCAT(Country, Year),
		ROW_NUMBER() OVER (PARTITION BY CONCAT(Country, Year) ORDER BY CONCAT(Country, Year)) AS ROW_NUM
	FROM world_life_expectancy
	) AS ROW_TABLE
	WHERE ROW_NUM > 1
);

#Identifies records where Status is blank (empty string).
#These need to be filled with either 'Developing' or 'Developed'.
select * 
from world_life_expectancy
where Status = '';

#Lists all unique values in the Status column, ignoring blanks.
#Helps determine the categories to use when filling missing values.
SELECT distinct Status
FROM world_life_expectancy
where Status <> '';

#Identifies all countries marked as 'Developing'.
#Useful for propagating the Status to missing rows for the same country.
SELECT distinct Country
FROM world_life_expectancy
where Status = 'Developing';

#Sets Status = 'Developing' for all rows of countries that have at least one 'Developing' record.
UPDATE world_life_expectancy
SET Status = 'Developing'
WHERE Country in (
		SELECT distinct (Country)
		FROM world_life_expectancy
		where Status = 'Developing'
);

#Joins the table to itself to see which rows have missing Status but have another row in the same country with a non-empty Status.
#Helps decide how to fill in missing values.
SELECT 
wfe_1.Country, 
wfe_1.Status,
wfe_2.Country,
wfe_2.Status
FROM world_life_expectancy as wfe_1
inner join world_life_expectancy wfe_2
    on wfe_1.Country = wfe_2.Country
where wfe_1.Status = ''
and wfe_2.Status <> ''; 

#Updates blank Status rows to 'Developing' if there’s another 'Developing' row for the same country.
UPDATE
world_life_expectancy wfe_1
join world_life_expectancy wfe_2
	on wfe_1.Country = wfe_2.Country
SET wfe_1.Status = 'Developing'
where wfe_1.Status = ''
AND wfe_2.Status <> ''
AND wfe_2.Status = 'Developing';

#Similar to the previous query, but fills blank Status rows as 'Developed'.
UPDATE
world_life_expectancy wfe_1
join world_life_expectancy wfe_2
	on wfe_1.Country = wfe_2.Country
SET wfe_1.Status = 'Developed'
where wfe_1.Status = ''
AND wfe_2.Status <> ''
AND wfe_2.Status = 'Developed';

#Ensures there are no more empty or NULL values in the Status column.
SELECT
	*
FROM world_life_expectancy
WHERE Status = '' 
or Status IS NULL;

#Finds rows where Life expectancy is blank.
#Plan is to fill missing values using the average of the previous and next year.
SELECT
	*
FROM world_life_expectancy
WHERE `Life expectancy` = '';
## returns 2 values- going to take the average of the previous year and subsequent year for blank or null values

#Basic query to inspect the Life expectancy column for potential cleaning.
SELECT
	Country,
    Year,
    `Life expectancy`
FROM world_life_expectancy
##WHERE `Life expectancy` = ''
;

#Joins the table to itself twice:
#t2 = previous year
#t3 = next year
#Computes the average of life expectancy from the surrounding years.
SELECT
	t1.Country,
    t1.Year,
    t1.`Life expectancy`,
    ROUND((t2.`Life expectancy` + t3.`Life expectancy`)/2, 1),
    t2.Country,
    t2.Year,
    t2.`Life expectancy`,
    t3.Country,
    t3.Year,
    t3.`Life expectancy`
FROM world_life_expectancy t1
INNER JOIN world_life_expectancy t2
	on t1.Country = t2.Country 
	and t1.Year = t2.Year -1
INNER JOIN world_life_expectancy t3
	on t1.Country = t3.Country
	AND t1.Year = t3.Year + 1
WHERE t1.`Life expectancy` = ''
;

#Updates the blank Life expectancy values using the average of previous and next years, rounded to 1 decimal.
UPDATE world_life_expectancy t1
INNER JOIN world_life_expectancy t2
	on t1.Country = t2.Country 
	and t1.Year = t2.Year -1
INNER JOIN world_life_expectancy t3
	on t1.Country = t3.Country
	AND t1.Year = t3.Year + 1
SET t1.`Life expectancy` =  ROUND((t2.`Life expectancy` + t3.`Life expectancy`)/2, 1)
WHERE t1.`Life expectancy` = ''
;

#Checks that all Life expectancy values are now populated.
SELECT
	Country,
    Year,
    `Life expectancy`
FROM world_life_expectancy
#WHERE `Life expectancy` = ''
;

