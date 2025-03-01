SELECT * FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','')  NOT IN (SELECT cst_key FROM silver.crm_cust_info);


SELECT DISTINCT CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		ELSE cntry END cntry 
FROM bronze.erp_loc_a101
ORDER BY 1;

INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT REPLACE(cid,'-','') AS cid, 
		CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		ELSE cntry END cntry 
FROM bronze.erp_loc_a101;

SELECT DISTINCT cntry 
FROM silver.erp_loc_a101
ORDER BY 1;

SELECT * FROM silver.erp_loc_a101;