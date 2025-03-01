CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===========================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===========================================================';

		PRINT '-----------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\Git\sql-data-warehouse-project\Resources\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		--cheking whether complete data loaded or not
		--SELECT * FROM bronze.crm_cust_info;
		--SELECT COUNT(*) FROM bronze.crm_cust_info;


		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Git\sql-data-warehouse-project\Resources\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		--cheking whether complete data loaded or not
		--SELECT * FROM bronze.crm_prd_info;
		--SELECT COUNT(*) FROM bronze.crm_prd_info;

		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Git\sql-data-warehouse-project\Resources\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		--cheking whether complete data loaded or not
		--SELECT * FROM bronze.crm_sales_details;
		--SELECT COUNT(*) FROM bronze.crm_sales_details;

		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\Git\sql-data-warehouse-project\Resources\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		--cheking whether complete data loaded or not
		--SELECT * FROM bronze.erp_cust_az12;
		--SELECT COUNT(*) FROM bronze.erp_cust_az12;


		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\Git\sql-data-warehouse-project\Resources\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		--cheking whether complete data loaded or not
		--SELECT * FROM bronze.erp_loc_a101;
		--SELECT COUNT(*) FROM bronze.erp_loc_a101;


		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\Git\sql-data-warehouse-project\Resources\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		--cheking whether complete data loaded or not
		--SELECT * FROM bronze.erp_px_cat_g1v2;
		--SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end_time = GETDATE();
		PRINT '===========================================================';
		PRINT 'Loading Bronze Layer is completed';
		PRINT '	- Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===========================================================';
	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	END CATCH

END;

GO


EXEC	bronze.load_bronze;
