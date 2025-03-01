
---Checking data quality

SELECT NULLIF(sls_order_dt,0) sls_order_dt 
FROM bronze.crm_sales_details  
WHERE sls_order_dt <= 0 ----issue exists
	OR LEN(sls_order_dt) != 8 ----issue exists
	OR sls_order_dt > 20500101
	OR sls_order_dt < 19000101;	


SELECT DISTINCT CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_price * sls_quantity != sls_sales 
				THEN ABS(sls_price) * sls_quantity ELSE sls_sales END sls_sales, 
	sls_quantity, 
	CASE WHEN sls_price IS NULL OR sls_price <=0 
				THEN sls_sales / NULLIF(sls_quantity,0) ELSE sls_price END sls_price
FROM bronze.crm_sales_details
WHERE (sls_price * sls_quantity != sls_sales
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
OR sls_price <=0  OR sls_quantity <=0   OR sls_sales <=0) --AND sls_price = 40
ORDER BY sls_sales, sls_quantity, sls_price ;



----Clean,Standardize, Transform, Insert data to silver schema table
INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
SELECT sls_ord_num, 
		sls_prd_key, 
		sls_cust_id, 
		CASE WHEN sls_order_dt IS NULL OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END sls_order_dt, 
		CASE WHEN sls_ship_dt IS NULL OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END sls_ship_dt, 
		CASE WHEN sls_due_dt IS NULL OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END sls_due_dt, 
		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_price * sls_quantity != sls_sales 
				THEN ABS(sls_price) * sls_quantity ELSE sls_sales END sls_sales, 
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <=0 
				THEN sls_sales / NULLIF(sls_quantity,0) ELSE sls_price END sls_price
FROM bronze.crm_sales_details;


SELECT * FROM silver.crm_sales_details;



---Checking data quality
SELECT *
FROM silver.crm_sales_details  
WHERE sls_order_dt > sls_ship_dt or sls_ship_dt > sls_due_dt;	


SELECT sls_sales, 
	sls_quantity, 
	sls_price
FROM silver.crm_sales_details
WHERE (sls_price * sls_quantity != sls_sales
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
OR sls_price <=0  OR sls_quantity <=0   OR sls_sales <=0) --AND sls_price = 40
ORDER BY sls_sales, sls_quantity, sls_price ;

