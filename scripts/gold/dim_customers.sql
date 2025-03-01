---Check duplicates customer after joining
SELECT t.cst_id, count(*)
FROM (SELECT cst_id , 
		cst_key, 
		cst_firstname, 
		cst_lastname , 
		cst_marital_status , 
		cst_gndr, 
		cst_create_date , 
		ca.bdate , 
		ca.gen ,
		la.cntry
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON (ci.cst_key = ca.cid)
LEFT JOIN silver.erp_loc_a101 la
ON (ci.cst_key = la.cid)) t
GROUP BY t.cst_id
HAVING COUNT(*) > 1;

---Same data field from different tables comparison
SELECT DISTINCT cst_gndr, 
		ca.gen 
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON (ci.cst_key = ca.cid)
LEFT JOIN silver.erp_loc_a101 la
ON (ci.cst_key = la.cid)
ORDER BY 1,2;

SELECT DISTINCT cst_gndr, 
		ca.gen,
		CASE WHEN cst_gndr != 'N/A' THEN cst_gndr  ---CRM is the master for gender info
			 ELSE COALESCE(ca.gen,cst_gndr) END gender
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON (ci.cst_key = ca.cid)
LEFT JOIN silver.erp_loc_a101 la
ON (ci.cst_key = la.cid)
ORDER BY 1,2;

-------------------------------------
SELECT cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
		cst_marital_status, 
		CASE WHEN cst_gndr != 'N/A' THEN cst_gndr  ---CRM is the master for gender info
			 ELSE COALESCE(ca.gen,cst_gndr) END  new_gen, 
		cst_create_date, 
		ca.bdate, 
		la.cntry
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON (ci.cst_key = ca.cid)
LEFT JOIN silver.erp_loc_a101 la
ON (ci.cst_key = la.cid);


---Renaming columns to user-friendly, meaningful names
---Dimension-Customer
CREATE OR ALTER VIEW gold.dim_customers 
AS
SELECT 
		ROW_NUMBER() OVER (ORDER BY cst_id) customer_key,
		cst_id AS customer_id, 
		cst_key AS customer_number, 
		cst_firstname AS first_name, 
		cst_lastname AS last_name, 
		la.cntry AS country, 
		cst_marital_status AS marital_status, 
		CASE WHEN cst_gndr != 'N/A' THEN cst_gndr  ---CRM is the master for gender info
			 ELSE COALESCE(ca.gen,cst_gndr) END  gender, 
		ca.bdate AS birthdate, 
		cst_create_date AS create_date
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON (ci.cst_key = ca.cid)
LEFT JOIN silver.erp_loc_a101 la
ON (ci.cst_key = la.cid);

---Checks the quality of gold dimension customer table
SELECT DISTINCT gender FROM gold.dim_customers;
SELECT DISTINCT country FROM gold.dim_customers