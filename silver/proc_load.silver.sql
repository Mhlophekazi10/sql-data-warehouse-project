CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
BEGIN TRY
    /* !!!!!CUSTOMER INFO !!!!!!*/
       /*CHECK FOR EMPTY SPOTS*/

        SELECT cst_gnde 
        FROM bronze.crm_cust_info
        WHERE cst_gnde != TRIM(cst_gnde)

    -- Check for Nulls & Duplicates in Primary Key
    SELECT 
    cst_id,
    COUNT(*)
    FROM bronze.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1 OR cst_id IS NULL

    -- Data Standardisation & consistency
    SELECT DISTINCT cst_gnde
    FROM bronze.crm_cust_info
        /******************/
        
        TRUNCATE TABLE silver.crm_cust_info;
        INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gnde, cst_create_date)
        SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
             WHEN UPPER(TRIM(cst_marital_status))  = 'M' THEN 'Married'
             ELSE 'n/a'
        END cst_marital_status,
        CASE WHEN UPPER(TRIM(cst_gnde)) = 'F' THEN 'Female'
             WHEN UPPER(TRIM(cst_gnde))  = 'M' THEN 'Male'
             ELSE 'n/a'
        END cst_gnde,
        cst_create_date
        FROM (
        SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info
         WHERE cst_id IS NOT NULL
        )t WHERE flag_last = 1

     /*CHECK FOR EMPTY SPOTS*/

        SELECT cst_gnde 
        FROM silver.crm_cust_info
        WHERE cst_gnde != TRIM(cst_gnde)

    -- Check for Nulls & Duplicates in Primary Key
    SELECT 
    cst_id,
    COUNT(*)
    FROM silver.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1 OR cst_id IS NULL

    -- Data Standardisation & consistency
    SELECT DISTINCT cst_gnde
    FROM silver.crm_cust_info

    SELECT * FROM silver.crm_cust_info

    --!!!PROCUCT INFO
    /*CHECK FOR EMPTY SPOTS*/

        SELECT prd_nam
        FROM bronze.crm_prd_info
        WHERE prd_nam != TRIM(prd_nam)

    -- Check for Nulls & Duplicates in Primary Key
    SELECT 
    prd_id,
    COUNT(*)
    FROM bronze.crm_prd_info
    GROUP BY prd_id
    HAVING COUNT(*) > 1 OR prd_id IS NULL

    -- Check for NULLS or negative NUMBERS
    SELECT prd_cost
        FROM silver.crm_prd_info
        WHERE prd_cost < 0 OR prd_cost IS NULL

    --Invalid dates
    SELECT *
    FROM silver.crm_prd_info
    WHERE prd_end_dt < prd_start_dt

    -- Data Standardisation & consistency
    SELECT DISTINCT prd_line
    FROM silver.crm_prd_info

    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nam, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key,
    prd_nam,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
         WHEN 'M' THEN 'Mountain'
         WHEN 'R' THEN 'Road'
         WHEN 'S' THEN 'Other Sales'
         WHEN 'T' THEN 'Touring'
         else 'n/a'
    END AS prd_line,
    prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE)AS prd_end_dt
    FROM bronze.crm_prd_info 

    /*!!!!!!! SALES DETAILS!!!!!!!!!*/
    -- Check for Nulls & Duplicates in Primary Key
    SELECT sls_cst_id
    FROM silver.crm_sales_details
    WHERE sls_cst_id > 1 OR sls_cst_id IS NULL

    -- Check for NULLS or negative NUMBERS
    SELECT DISTINCT
    sls_sales AS oldsls_sales,
    sls_quantity,
    sls_price AS oldsls_price
    FROM silver.crm_sales_details
    WHERE sls_sales != sls_quantity * sls_price OR 
        sls_sales IS NULL OR sls_price IS NULL OR sls_quantity IS NULL OR
        sls_sales <= 0 OR sls_price <=0 OR sls_quantity <=0
        ORDER BY sls_sales, sls_quantity, sls_price

    --Invalid dates!
    SELECT NULLIF(sls_due_dt,0) sls_due_dt
    FROM silver.crm_sales_details
    WHERE sls_due_dt <= 0 OR LEN(sls_due_dt)!=8 

    SELECT * 
    FROM silver.crm_sales_details
    WHERE sls_ord_dt > sls_ship_dt OR sls_ord_dt > sls_due_dt

    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cst_id, sls_ord_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cst_id,
    CASE WHEN sls_ord_dt <= 0 OR LEN(sls_ord_dt)!=8 THEN NULL
           ELSE CAST (CAST(sls_ord_dt AS VARCHAR) AS DATE)
    END AS  sls_ord_dt,
    CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt)!=8 THEN NULL
           ELSE CAST (CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS  sls_ship_dt,
    CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt)!=8 THEN NULL
           ELSE CAST (CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS  sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
             THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales / NULLIF( sls_quantity , 0)
         ELSE sls_sales 
    END AS sls_price
    FROM bronze.crm_sales_details
    SELECT * FROM silver.crm_sales_details

    /*!!!!!!ERP CUST_AZ_12!!!!!!*/
    --INVALID BIRTHDAY
    SELECT DISTINCT
    b_date
    FROM bronze.erp_cust_az12
    WHERE b_date < '1924-01-01' OR b_date > GETDATE()

    -- DATA STANDARDISATION & CONSISTENCY
    SELECT DISTINCT gndr
    FROM bronze.erp_cust_az12

    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 ( c_id, b_date, gndr)
    SELECT 
    CASE WHEN c_id LIKE 'NAS%' THEN SUBSTRING(c_id, 4, LEN(c_id))
        ELSE c_id
    END c_id,
    CASE WHEN b_date > GETDATE() THEN NULL
        ELSE b_date
    END AS b_date,
    CASE WHEN UPPER(TRIM(gndr)) IN ('F' , 'FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gndr)) IN ('M' , 'MALE') THEN 'Male'
         ELSE 'N/A'
    END AS gndr
    FROM bronze.erp_cust_az12

    --ERP LOC

    SELECT DISTINCT cntry
    FROM silver.erp_loc_a101
    ORDER BY cntry

    TRUNCATE TABLE silver.erp_loc_a101
    INSERT INTO silver.erp_loc_a101 (c_id, cntry)
    SELECT 
    REPLACE(c_id,'-','') c_id, 
    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
         ELSE TRIM(cntry)
    END AS cntry
    FROM bronze.erp_loc_a101

    --ERP PX
    --CHECK FOR UNWANTED SPACES
    SELECT * FROM silver.erp_px_cat_c1v2
    WHERE cat != TRIM(cat) OR sub_cat != TRIM(sub_cat) OR maintenence != TRIM(maintenence)

    -- DATA STANDARDISATION & CONSISTENCY
    SELECT DISTINCT
    maintenence
    FROM silver.erp_px_cat_c1v2

    TRUNCATE TABLE silver.erp_px_cat_c1v2;
    INSERT INTO silver.erp_px_cat_c1v2 (id, cat, sub_cat, maintenence)
    SELECT 
    id,
    cat,
    sub_cat,
    maintenence
    FROM bronze.erp_px_cat_c1v2
    END TRY
    BEGIN CATCH
    PRINT 'THIS IS AN ERROR MESSAGE'
    END CATCH;
END;
