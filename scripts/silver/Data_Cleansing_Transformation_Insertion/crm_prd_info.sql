---Checks for Nulls or Duplication in Primary Key
---Expectation: No Result

SELECT prd_id, COUNT(1)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(1) > 1 OR prd_id IS NULL;


---Checks for Unwanted Spaces
---Expectation: No Result
SELECT prd_key
FROM bronze.crm_prd_info
WHERE prd_key != TRIM(prd_key);

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);



----Check for null or negative number
---Expectation: No Result
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

---Check for Invalid Date order
SELECT * FROM bronze.crm_prd_info
where prd_start_dt > prd_end_dt

----------------------------------------------------------------
----Checks for low cardinality columns
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

----Checks for valid products, unused products for ordering or sales
SELECT prd_id,
		prd_key,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') prd_cat,
		SUBSTRING(prd_key,7,LEN(prd_key)-6) prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
FROM bronze.crm_prd_info p
WHERE NOT EXISTS (SELECT 1 FROM bronze.crm_sales_details s WHERE SUBSTRING(prd_key,7,LEN(prd_key)-6) = s.sls_prd_key);


SELECT prd_id,
		prd_key,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') prd_cat,
		SUBSTRING(prd_key,7,LEN(prd_key)-6) prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
FROM bronze.crm_prd_info p
WHERE NOT EXISTS (SELECT 1 FROM bronze.erp_px_cat_g1v2 s WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') = s.id);

----------------
INSERT INTO silver.crm_prd_info(
							prd_id, 
							prd_cat, 
							prd_key,
							prd_nm, 
							prd_cost, 
							prd_line, 
							prd_start_dt, 
							prd_end_dt)
SELECT prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS prd_cat,
		SUBSTRING(prd_key,7,LEN(prd_key)-6) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost, 
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'N/A' END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info p;

-----Checks the issues found in the bronze schema table

----Check for null or negative number
---Expectation: No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

----Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

---Check for Invalid date order
SELECT * FROM silver.crm_prd_info
where prd_start_dt > prd_end_dt