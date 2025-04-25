###this script create tables in bronze layer ,droping tables if there exist and recreate it

#------------------------------------------------------create cust_info table

if object_id ('Bronze.crm_cust_info','u') is not null
       drop table Bronze.crm_cust_info;
create table Bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);


#--------------------------------------------------------create prd_info table

if object_id ('Bronze.crm_prd_info','u') is not null
       drop table Bronze.crm_prd_info;
create table Bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost numeric,
prd_line nvarchar(2),
prd_start_dt date,
prd_end_dt date
);


#---------------------------------------------------------------create sales_details table

if object_id ('Bronze.crm_sales_details','u') is not null
       drop table Bronze.crm_sales_details;
create table Bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales numeric,
sls_quantity numeric,
sls_price numeric
);

#-------------------------------------------------------create cust_az12 table

if object_id ('Bronze.erp_cust_az12','u') is not null
       drop table Bronze.erp_cust_az12;
create table Bronze.erp_cust_az12(
CID nvarchar(50),
BDATE date,
GEN nvarchar(50)
);

#----------------------------------------------------------create loc_a101 table

if object_id ('Bronze.erp_loc_a101','u') is not null
       drop table Bronze.erp_loc_a101;
create table Bronze.erp_loc_a101(
CID nvarchar(50),
CNTRY nvarchar(50)
);

#-----------------------------------------------------------create   px_g1v2 table

if object_id ('Bronze.erp_px_g1v2','u') is not null
       drop table Bronze.erp_px_g1v2;
create table Bronze.erp_px_g1v2(
ID nvarchar(10),
CAT nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE nvarchar(5)
);
