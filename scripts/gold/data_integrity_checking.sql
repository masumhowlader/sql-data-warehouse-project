---Foreign Key Integrity (Dimensions)
SELECT * 
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
WHERE (c.customer_key IS NULL OR p.product_key IS NULL)