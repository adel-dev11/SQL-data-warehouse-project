/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

if object_id ('Silver.crm_cust_info','u') is not null
       drop table Silver.crm_cust_info;
create table Silver.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate()
);


--------------------------------------------------------create prd_info table

if object_id ('Silver.crm_prd_info','u') is not null
       drop table Silver.crm_prd_info;
CREATE TABLE Silver.crm_prd_info(
    prd_id int,
    prd_key nvarchar(50),
    category_id nvarchar(50),      
    product_key nvarchar(50),       
    prd_nm nvarchar(50),
    prd_cost numeric,
    prd_line nvarchar(50),          
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_date datetime2 default getdate()
);


---------------------------------------------------------------create sales_details table

if object_id ('Silver.crm_sales_details','u') is not null
       drop table Silver.crm_sales_details;
create table Silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales numeric,
sls_quantity numeric,
sls_price numeric,
dwh_create_date datetime2 default getdate()
);

-------------------------------------------------------create cust_az12 table

if object_id ('Silver.erp_cust_az12','u') is not null
       drop table Silver.erp_cust_az12;
create table Silver.erp_cust_az12(
CID nvarchar(50),
BDATE date,
GEN nvarchar(50),
dwh_create_date datetime2 default getdate()
);

----------------------------------------------------------create loc_a101 table

if object_id ('Silver.erp_loc_a101','u') is not null
       drop table Silver.erp_loc_a101;
create table Silver.erp_loc_a101(
CID nvarchar(50),
CNTRY nvarchar(50),
dwh_create_date datetime2 default getdate()
);

-----------------------------------------------------------create   px_g1v2 table

if object_id ('Silver.erp_px_g1v2','u') is not null
       drop table Silver.erp_px_g1v2;
create table Silver.erp_px_g1v2(
ID nvarchar(10),
CAT nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE nvarchar(5),
dwh_create_date datetime2 default getdate()
);
