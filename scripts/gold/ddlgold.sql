--**CREATE VIEWS FOR THE GOLD LAYER WHICH REPRESENTS THE FINAL DIMENSION & FACT TABLES USING STAR SCHEMA**--

CREATE VIEW gold.dim_customers AS 
SELECT
        ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
        la.cntry AS country,
        CASE WHEN ci.cst_gnde != 'n/a' THEN ci.cst_gnde
        ELSE  COALESCE(ca.gndr, 'N/A')
        END AS gender,
		ci.cst_marital_status AS marital_status,
        ca.b_date AS birth_date,
		ci.cst_create_date AS create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON		ci.cst_key = ca.c_id
    LEFT JOIN silver.erp_loc_a101 la
	ON		ci.cst_key = la.c_id
    
    --GENDER INTEGRATION
    SELECT DISTINCT
		ci.cst_gnde,
		ca.gndr,
        CASE WHEN ci.cst_gnde != 'n/a' THEN ci.cst_gnde
        ELSE  COALESCE(ca.gndr, 'N/A')
        END AS new_gen
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON		ci.cst_key = ca.c_id
    LEFT JOIN silver.erp_loc_a101 la
	ON		ci.cst_key = la.c_id
    ORDER BY 1,2

    CREATE VIEW gold.dim_product AS
    SELECT  
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nam AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.sub_cat AS subcategory,
    pc.maintenence,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_c1v2 pc
    ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL

    CREATE VIEW gold.fact_sales AS
    SELECT 
	sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_id,
	sd.sls_ord_dt AS order_date,
	sd.sls_ship_dt AS shippping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
	FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_product pr
    ON sd.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customers cu
    ON sd.sls_cst_id = cu.customer_id
