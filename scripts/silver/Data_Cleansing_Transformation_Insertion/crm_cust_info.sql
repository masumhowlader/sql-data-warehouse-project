---Checks for Nulls or Duplication in Primary Key
---Expectation: No Result

SELECT cst_id, COUNT(1)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(1) > 1 OR cst_id IS NULL;

SELECT * FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info )as ci WHERE flag_last != 1;


---Checks for Unwanted Spaces
---Expectation: No Result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);


----Check the consistency of values in low cardinality columns

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;


----Correction and Transformation and insertion in the silver schema table
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


SELECT * FROM silver.crm_cust_info

-----Checks the issues found in the bronze schema table

---Checks for Nulls or Duplication in Primary Key
---Expectation: No Result

SELECT cst_id, COUNT(1)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(1) > 1 OR cst_id IS NULL;

SELECT * FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM silver.crm_cust_info )as ci WHERE flag_last != 1;


---Checks for Unwanted Spaces
---Expectation: No Result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

----Check the Data Standardization & Consistency
		----Maps coded values to meaningful, user-friendly descriptions
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;





---Data Cleansing, Normalization & Standardization, Transformation
---1. Remove unwanted space; for example ' Sophia' to 'Sophia' or 'Alben ' to 'Alben'
---2. Remove duplicate rows for example more than 1 row for cst_id; for example 2 records with same cst_id=92342
---3. Remove unidentifying data for example cst_id is null
---4. Coded value transform to meaningful, user-friendly description; for xample cst_gndr: M=Male; F=Female
---5. Handle missing value with a default value; for example cst_gndr is null to 'N/A'
