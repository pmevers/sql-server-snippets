-- CTE
With dupes as (SELECT *, ROW_NUMBER() OVER (PARTITION BY Key_Values ORDER BY (SELECT NULL)) AS RowNum FROM Source_Table),
SELECT COUNT(*) FROM dupes -- Sometimes select the identity fields to do some profiling or debugging
WHERE RowNum > 1

-- Without CTE

SELECT COUNT(*) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Key_Values ORDER BY (SELECT NULL)) AS RowNum FROM Source_Table) t
WHERE RowNum > 1

-- Deduplication
DELETE dupes FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Key_Values ORDER BY (SELECT NULL)) AS RowNum FROM Source_Table) dupes
