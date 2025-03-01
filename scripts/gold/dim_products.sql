--Duplicate data checking
SELECT prd_key, COUNT(*) FROM (
SELECT 
	prd_id,
	prd_cat,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON (pn.prd_cat = pc.id)
WHERE pn.prd_end_dt IS NULL ---Filter out the historical data
) T
GROUP BY prd_key
HAVING COUNT(*) > 1;

-------------------------

CREATE OR ALTER VIEW gold.dim_products
AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY prd_start_dt,prd_key) product_key,
	prd_id AS product_id,
	prd_key AS  product_number,
	prd_nm  AS product_name,
	pc.id  AS category_id,
	pc.cat AS category,
	pc.subcat AS sub_category,
	pc.maintenance,
	prd_cost AS product_cost,
	prd_line AS prdoduct_line,
	prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON (pn.prd_cat = pc.id)
WHERE pn.prd_end_dt IS NULL;

----

--Quality checking
SELECT * FROM gold.dim_products