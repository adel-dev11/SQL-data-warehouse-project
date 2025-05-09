/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
===============================================================================
*/
CREATE OR ALTER PROCEDURE Silver.load_silver
AS
BEGIN
    BEGIN TRY
        DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        PRINT 'Loading silver layer'
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

        SET @batch_start_time = GETDATE();

        ----------------- Load 1: crm_cust_info -----------------
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        PRINT 'Loading Silver.crm_cust_info'
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        SET @start_time = GETDATE();

        TRUNCATE TABLE Silver.crm_cust_info;

        INSERT INTO Silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date)
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'N/A'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'N/A'
            END,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
            FROM Bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag = 1;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------'

        ----------------- Load 2: crm_prd_info -----------------
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        PRINT 'Loading Silver.crm_prd_info'
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        SET @start_time = GETDATE();

        INSERT INTO Silver.crm_prd_info (
            prd_id, prd_key, category_id, product_key,
            prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        SELECT
            prd_id,
            prd_key,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            TRIM(prd_nm),
            ISNULL(prd_cost, 0),
            CASE
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'N/A'
            END,
            CAST(prd_start_dt AS DATE),
            DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
        FROM Bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------'

        ----------------- Load 3: crm_sales_details -----------------
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        PRINT 'Loading Silver.crm_sales_details'
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        SET @start_time = GETDATE();

        INSERT INTO Silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price)
        SELECT 
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            CASE
                WHEN sls_sales != sls_quantity * ABS(sls_price)
                     OR sls_sales IS NULL
                     OR sls_sales <= 0
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END
        FROM Bronze.crm_sales_details
        WHERE sls_order_dt IS NOT NULL AND sls_order_dt >= '2000-01-01';

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------'

        ----------------- Load 4: erp_cust_az12 -----------------
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        PRINT 'Loading Silver.erp_cust_az12'
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        SET @start_time = GETDATE();

        INSERT INTO Silver.erp_cust_az12 (CID, BDATE, GEN)
        SELECT 
            CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID)) ELSE CID END,
            CASE WHEN BDATE > GETDATE() THEN NULL ELSE BDATE END,
            CASE 
                WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'N/A'
            END
        FROM Bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------'

        ----------------- Load 5: erp_loc_a101 -----------------
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        PRINT 'Loading Silver.erp_loc_a101'
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        SET @start_time = GETDATE();

        INSERT INTO Silver.erp_loc_a101 (CID, CNTRY)
        SELECT 
            REPLACE(CID, '-', ''),
            CASE 
                WHEN UPPER(TRIM(CNTRY)) IN ('USA', 'US', 'UNITED STATES') THEN 'United States'
                WHEN UPPER(TRIM(CNTRY)) IN ('DE', 'GERMANY') THEN 'German'
                WHEN UPPER(TRIM(CNTRY)) = 'AUSTRALIA' THEN 'Australia'
                WHEN UPPER(TRIM(CNTRY)) = 'UNITED KINGDOM' THEN 'United Kingdom'
                WHEN UPPER(TRIM(CNTRY)) = 'CANADA' THEN 'Canada'
                WHEN UPPER(TRIM(CNTRY)) = 'FRANCE' THEN 'France'
                ELSE 'N/A'
            END
        FROM Bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------'

        ----------------- Load 6: erp_px_g1v2 -----------------
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        PRINT 'Loading Silver.erp_px_g1v2'
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        SET @start_time = GETDATE();

        INSERT INTO Silver.erp_px_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)
        SELECT ID, CAT, SUBCAT, MAINTENANCE
        FROM Bronze.erp_px_g1v2;

        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------'

        ----------------- Batch End -----------------
        SET @batch_end_time = GETDATE();
        PRINT 'Total silver layer load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR);
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

    END TRY
    BEGIN CATCH
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        PRINT 'Error in loading silver layer'
        PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        PRINT 'Error message: ' + ERROR_MESSAGE();
        PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error state: ' + CAST(ERROR_STATE() AS NVARCHAR);
    END CATCH
END;
