SELECT cid,
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
		END cid,
		bdate,
		gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
		END NOT IN (SELECT cst_key FROM silver.crm_cust_info);

---Identifying Out-of-Range date
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();


SELECT DISTINCT gen
FROM bronze.erp_cust_az12;


----Insert into silver schema table
INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen)
SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
		END cid,
		CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN  'Male'
			 WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN  'Female'
		ELSE 'N/A' END gen
FROM bronze.erp_cust_az12;


---Outdated date range		
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();


SELECT DISTINCT gen
FROM silver.erp_cust_az12;


SELECT * FROM silver.erp_cust_az12;