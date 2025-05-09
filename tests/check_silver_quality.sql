/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
-- ============================================
-- 📌 crm_cust_info Cleaning & Transformation
-- ============================================

-- Remove duplicates by keeping the latest entry
WITH duplicate_id AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num
  FROM Bronze.crm_cust_info
)
DELETE FROM duplicate_id WHERE row_num > 1;

-- Trim names and standardize marital status & gender
INSERT INTO Silver.crm_cust_info (
  cst_id, cst_key, cst_firstname, cst_lastname,
  cst_marital_status, cst_gndr, cst_create_date
)
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
FROM Bronze.crm_cust_info
WHERE cst_id IS NOT NULL;

-- ============================================
-- 📌 crm_prd_info Cleaning & Transformation
-- ============================================

INSERT INTO Silver.crm_prd_info (
  prd_id, prd_key, category_id, product_key, prd_nm,
  prd_cost, prd_line, prd_start_dt, prd_end_dt
)
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

-- ============================================
-- 📌 crm_sales_details Cleaning & Transformation
-- ============================================

INSERT INTO Silver.crm_sales_details (
  sls_ord_num, sls_prd_key, sls_cust_id,
  sls_order_dt, sls_ship_dt, sls_due_dt,
  sls_sales, sls_quantity, sls_price
)
SELECT 
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
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

-- ============================================
-- 📌 erp_cust_az12 Cleaning & Transformation
-- ============================================

INSERT INTO Silver.erp_cust_az12 (CID, BDATE, GEN)
SELECT 
  CASE 
    WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
    ELSE CID
  END,
  CASE 
    WHEN BDATE > GETDATE() THEN NULL
    ELSE BDATE
  END,
  CASE 
    WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
    ELSE 'N/A'
  END
FROM Bronze.erp_cust_az12;

-- ============================================
-- 📌 erp_loc_a101 Cleaning & Transformation
-- ============================================

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

-- ============================================
-- 📌 erp_px_g1v2 Cleaning & Transformation
-- ============================================

-- Direct copy without transformation
INSERT INTO Silver.erp_px_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)
SELECT ID, CAT, SUBCAT, MAINTENANCE
FROM Bronze.erp_px_g1v2;
