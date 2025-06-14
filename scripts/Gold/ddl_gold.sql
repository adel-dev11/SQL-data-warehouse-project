/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


----create view for dimensional customer table

create view Gold.dim_customers as 
    select 
	ROW_NUMBER() over (order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	ci.cst_marital_status as marital_status,
	la.CNTRY as country,
	case when ci.cst_gndr != 'N/A' then ci.cst_gndr 
	else coalesce(ca.GEN,'N/A')
	end as gender,
	ca.BDATE as birthdate,
	ci.cst_create_date as create_date
from Silver.crm_cust_info ci 
left join Silver.erp_cust_az12 ca
on ci.cst_key=ca.CID
left join Silver.erp_loc_a101 la
on ci.cst_key=la.CID

----create view for dimensional product table

create view Gold.dim_product as
	select 
	ROW_NUMBER() over (order by pn.prd_start_dt, pn.product_key) as product_okey,
	pn.prd_id as product_id,
	pn.product_key as product_key,
	pn.prd_nm as product_number,
	pn.category_id as category_id,
	pe.CAT as category,
	pe.SUBCAT as sub_category,
	pe.MAINTENANCE as maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as p_start_date
	from Silver.crm_prd_info pn
	left join Silver.erp_px_g1v2 pe
	on pn.category_id = pe.ID
	where pn.prd_end_dt is null  


  ----create view for fact sales table

create view Gold.fact_sales as
	select
	sd.sls_ord_num as order_number,
	gdp.product_okey,
	gdc.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales,
	sd.sls_quantity as quantity,
	sd.sls_price AS price
	from Silver.crm_sales_details sd
	left join Gold.dim_customers gdc
	on sd.sls_cust_id = gdc.customer_id
	left join Gold.dim_product gdp
	on sd.sls_prd_key = gdp.product_key
