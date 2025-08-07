CREATE OR ALTER  PROCEDURE bronze.load_bronze AS
    BEGIN
       DECLARE @start_time DATETIME, @end_time DATETIME;
       DECLARE @start_time1 DATETIME, @end_time1 DATETIME;
       SET @start_time1 = GETDATE();
       BEGIN TRY
        PRINT '*********************';
        PRINT 'Loading bronze layer';
        PRINT '*********************';

        PRINT '*********************';
        PRINT 'Loading CRM Tables'; 
        PRINT '*********************';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\buhle\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '------ Table: bronze.crm_cust_info LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\buhle\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
         SET @end_time = GETDATE();
        PRINT '------ Table: bronze.crm_prd_info LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\buhle\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
         SET @end_time = GETDATE();
        PRINT '------ Table: bronze.crm_sales_details LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds------';

        PRINT '*********************';
        PRINT 'Loading ERP Layer'; 
        PRINT '*********************';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\buhle\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
         SET @end_time = GETDATE();
        PRINT '------ Table: bronze.erp_cust_az12 LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\buhle\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
         SET @end_time = GETDATE();
        PRINT '------ Table: bronze.erp_loc_a101 LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_c1v2;
        BULK INSERT bronze.erp_px_cat_c1v2
        FROM 'C:\Users\buhle\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
         SET @end_time = GETDATE();
        PRINT '------ Table: bronze.erp_px_cat_c1v2 LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds------';

        END TRY
        BEGIN CATCH 
            PRINT '******** ERROR OCCURED DURING LOADING********'
        END CATCH
        SET @end_time1 = GETDATE();
        PRINT '------ BRONZE LAYER LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time1, @end_time1) AS NVARCHAR) + ' seconds------';
    END
