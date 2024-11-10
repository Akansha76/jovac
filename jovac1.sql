--Visualizing data
select * from prod_data pd

--Checking Duplicate values
select *, count(*) from prod_data pd group by "Production Date", "Production Level", "Location", "Product Type"
having count(*)>1

--Creating CTC for selecting Duplicate values
WITH del_dup AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY "Production Date", "Production Level", "Location" ORDER BY "Product Type") AS Dup
    FROM prod_data pd
)
SELECT * 
FROM del_dup
WHERE Dup > 1;


--Invalid Product Type
select * from prod_data where "Product Type" not in ('Associated Natural Gas Liquids', 'Crude Oil', 'Natural Gas')

--Deleting Invalid Product Type
delete from prod_data where "Product Type" not in ('Associated Natural Gas Liquids', 'Crude Oil', 'Natural Gas')


--Deleting Null Product Type
delete from prod_data where "Product Type" is NULL

--Creating CTE for detecting Outliers
WITH prod_stats AS (
    SELECT 
        "Location", 
        "Product Type", 
        AVG("Production Level") AS Avg_ProdLvl,
        STDDEV("Production Level") AS Std_Dev
    FROM prod_data 
    GROUP BY "Product Type", "Location"
),
prod_outlier AS (
    SELECT 
        prod."Location", 
        prod."Production Level", 
        prod."Product Type", 
        (prod."Production Level" - stat.Avg_ProdLvl) / stat.Std_Dev AS ZScore
    FROM prod_data prod
    JOIN prod_stats stat 
        ON prod."Product Type" = stat."Product Type" 
        AND prod."Location" = stat."Location"
),
outlierDetect AS (
    SELECT *, 
           CASE 
               WHEN ZScore > 1.96 OR ZScore < -1.96 THEN 1 
               ELSE 0 
           END AS OutlierDet
    FROM prod_outlier
)
SELECT *     --Selecting the rows having outliers
FROM prod_data prod
JOIN outlierDetect od
    ON prod."Production Level" = od."Production Level"
   AND prod."Product Type" = od."Product Type"
   AND prod."Location" = od."Location"
WHERE od.OutlierDet = 1;


--Deleting the outliers
WITH prod_stats AS (
    SELECT 
        "Location", 
        "Product Type", 
        AVG("Production Level") AS Avg_ProdLvl,
        STDDEV("Production Level") AS Std_Dev
    FROM prod_data 
    GROUP BY "Product Type", "Location"
),
prod_outlier AS (
    SELECT 
        prod."Location", 
        prod."Production Level", 
        prod."Product Type", 
        (prod."Production Level" - stat.Avg_ProdLvl) / stat.Std_Dev AS ZScore
    FROM prod_data prod
    JOIN prod_stats stat 
        ON prod."Product Type" = stat."Product Type" 
        AND prod."Location" = stat."Location"
),
outlierDetect AS (
    SELECT 
        prod.ctid,
        CASE 
            WHEN (prod."Production Level" - stat.Avg_ProdLvl) / stat.Std_Dev > 1.96 OR 
                 (prod."Production Level" - stat.Avg_ProdLvl) / stat.Std_Dev < -1.96 THEN 1 
            ELSE 0 
        END AS OutlierDet
    FROM prod_data prod
    JOIN prod_stats stat 
        ON prod."Product Type" = stat."Product Type"
        AND prod."Location" = stat."Location"
)
DELETE FROM prod_data
WHERE ctid IN (
    SELECT ctid FROM outlierDetect WHERE OutlierDet = 1
);


-- Checking Duplicate values
SELECT *, COUNT(*) 
FROM prod_data 
GROUP BY "Production Date" ,"Production Level", "Location", "Product Type"
HAVING COUNT(*) > 1;


-- Creating CTE for selecting Duplicate values
WITH del_dup AS (
    SELECT *, 
           ROW_NUMBER() OVER (
               PARTITION BY "Production Date", "Production Level", "Location", "Product Type" 
               ORDER BY "Product Type"
           ) AS Dup 
    FROM prod_data
)
SELECT * 
FROM del_dup
WHERE Dup > 1;


--Deleting duplicate values
WITH del_dup AS (
    SELECT ctid, 
           ROW_NUMBER() OVER (
               PARTITION BY "Production Date", "Production Level", "Location", "Product Type" 
               ORDER BY "Product Type"
           ) AS Dup 
    FROM prod_data
)
DELETE FROM prod_data
WHERE ctid IN (
    SELECT ctid FROM del_dup WHERE Dup > 1
);


--Looking the data having location = null
select * from prod_data where "Location" is null

--Deleting the data having location = null
delete from prod_data where "Location" is null


--Calculating Avg		
WITH avg_cte AS (
    SELECT "Location", "Product Type", AVG("Production Level") AS Avg_ProdLvl
    FROM prod_data 
    GROUP BY "Product Type", "Location"
)
SELECT * 
FROM avg_cte;
		
--Mean Imputation 
WITH avg_cte AS (
    SELECT 
        "Location", 
        "Product Type", 
        AVG("Production Level") AS Avg_ProdLvl
    FROM prod_data 
    GROUP BY "Product Type", "Location"
)

UPDATE prod_data prod
SET "Production Level" = avg_cte.Avg_ProdLvl
FROM avg_cte
WHERE prod."Product Type" = avg_cte."Product Type"
  AND prod."Location" = avg_cte."Location"
  AND prod."Production Level" IS NULL;

-- Checking whether the rows are updated or not
WITH avg_cte AS (
    SELECT 
        "Location", 
        "Product Type", 
        AVG("Production Level") AS Avg_ProdLvl
    FROM prod_data 
    GROUP BY "Product Type", "Location"
)
SELECT 
    od."Location", 
    od."Product Type", 
    prod."Production Level", 
    COALESCE(prod."Production Level", od.Avg_ProdLvl) AS Updated_ProductionLevel
FROM avg_cte od
JOIN prod_data prod 
    ON prod."Product Type" = od."Product Type" 
    AND prod."Location" = od."Location";

-- Altering the table column
ALTER TABLE prod_data 
ALTER COLUMN "Production Date" TYPE date 
USING TO_DATE("Production Date", 'MM/DD/YYYY');


