CREATE VIEW gold.fact_sales
AS
SELECT sls_ord_num AS order_number,
		pr.product_key,
		cu.customer_key,
		sls_order_dt AS order_date,
		sls_ship_dt AS shipping_date,
		sls_due_dt AS due_date,
		sls_sales AS sales_amount,
		sls_quantity AS quantity,
		sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;