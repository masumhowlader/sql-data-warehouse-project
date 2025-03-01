EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATE, @batch_end_time DATE;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Loading Silver Layer';
		PRINT '=========================================';

		PRINT '-----------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
		cst_id,cst_key, cst_firstname, cst_lastname, cst_gndr, cst_marital_status, cst_create_date)
		SELECT uci.cst_id, 
			uci.cst_key, 
			TRIM(uci.cst_firstname) AS cst_firstname, 
			TRIM(uci.cst_lastname) AS cst_lastname, 
			CASE WHEN UPPER(TRIM(uci.cst_gndr)) = 'M' THEN 'Male'  ----Data Standardization----UPPER(TRIM(uci.cst_gndr)) instead of uci.cst_gndr when we cannot trust on the column data
				 WHEN UPPER(TRIM(uci.cst_gndr)) = 'F' THEN 'Female'
				 ELSE 'N/A' END cst_gndr,	---Handling missing values with a default value
			CASE UPPER(TRIM(uci.cst_marital_status)) 
				 WHEN 'S' THEN 'Single'  ----Data Standardization----UPPER(TRIM(uci.cst_gndr)) instead of uci.cst_gndr when we cannot trust on the column data
				 WHEN 'M' THEN 'Married'
				 ELSE 'N/A' END cst_marital_status,		---Handling missing values with a default value
			uci.cst_create_date
		FROM (SELECT * FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL)as ci WHERE flag_last = 1) AS uci;
		SET @end_time = GETDATE();
		PRINT 'Loading Table crm_cust_info is Completed. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
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
		SET @end_time = GETDATE();
		PRINT 'Loading Table crm_prd_info is Completed. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
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
		SET @end_time = GETDATE();
		PRINT 'Loading Table crm_prd_info is Completed. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		PRINT '=========================================';
		PRINT 'Loading ERP Tables';
		PRINT '=========================================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
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
		SET @end_time = GETDATE();
		PRINT 'Loading Table crm_prd_info is Completed. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(cid, cntry)
		SELECT REPLACE(cid,'-','') AS cid, 
				CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
					WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
					WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
				ELSE cntry END cntry 
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT 'Loading Table crm_prd_info is Completed. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
		SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT 'Loading Table crm_prd_info is Completed. Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT 'Error occured';
	END CATCH
END;
