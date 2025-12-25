/*
====================================================================================
Quality Checks
====================================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standardization across the 'Bronze'and 'silver' schemas. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading Bronze and Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
======================================================================================

*/
--=============================================================
-- checking for  bronze crm_cust_info and silver crm_cust_info 
--=============================================================

----------------------------------
-- checking : bronze crm_cust_info
----------------------------------
-- check for null or duplicate in primary key
-- Expectation : no result 

select * from bronze.crm_cust_info;

select cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

-- checking unwanted spaces
-- Expectation : No Results
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname);

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname);

select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr);

select cst_material_status
from bronze.crm_cust_info
where cst_material_status != trim(cst_material_status);

select cst_key
from bronze.crm_cust_info
where cst_key != trim(cst_key)

-- Data Standardization & Consistency
select distinct cst_gndr
from bronze.crm_cust_info;
------------------------------------
-- checking : silver.crm_cust_info
------------------------------------
-- check for null or duplicate in primary key
-- Expectation : no result 

select cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;


-- checking unwanted spaces
-- Expectation : No Results
select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);


select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);


select cst_gndr
from silver.crm_cust_info
where cst_gndr != trim(cst_gndr);


select cst_material_status
from silver.crm_cust_info
where cst_material_status != trim(cst_material_status);

select cst_key
from silver.crm_cust_info
where cst_key != trim(cst_key)

-- Data Standardization & Consistency
select distinct cst_gndr
from silver.crm_cust_info;

-- ============================================================
-- checking for  bronze cust_prd_info and Silver_cust_prd_info
--=============================================================

-----------------------------------
-- checking : bronze cust_prd_info
-----------------------------------
-- check for null or duplicate in primary key
-- Expectation : no result 

select * from bronze.crm_prd_info;

select prd_id,
count(*)
from bronze.crm_prd_info
group by  prd_id
having count(*) > 1 or prd_id is null;


-- checking unwanted spaces
-- Expectation : No Results

select prd_nm
from bronze.crm_prd_info
where  prd_nm != trim(prd_nm);

-- Data Standardization & Consistency

select distinct prd_line
from bronze.crm_prd_info;

-------------------------------------------
-- checking : Silver_cust_prd_info
-------------------------------------------
-- check for null or duplicate in primary key
-- Expectation : no result 
select prd_id,
count(*)
from silver.crm_prd_info
group by  prd_id
having count(*) > 1 or prd_id is null;


-- checking unwanted spaces
-- Expectation : No Results

select prd_nm
from silver.crm_prd_info
where  prd_nm != trim(prd_nm);

-- Data Standardization & Consistency

select distinct prd_line
from silver.crm_prd_info;

-- check for null or negative numbers
-- Expectation : no result 
select prd_cost
from silver.crm_prd_info
where prd_cost < 0  or prd_id is null;

-- check for Invalid Date Orders
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;

-- Final check
select * from silver.crm_prd_info;

-- ============================================================================
-- checking for bronze.crm_sales_details and Checking silver.crm_sales_details
-- ============================================================================

---------------------------------------
-- checking : bronze.crm_sales_details
---------------------------------------
select 
sls_ord_num,
sls_prd_key ,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price 
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num) or
sls_cust_id not in (select cst_id from silver.crm_cust_info) or 
sls_prd_key not in (select prd_key from silver.crm_prd_info);


-- check for invalid dates

select
sls_order_dt 
from bronze.crm_sales_details

select
sls_order_dt 
from bronze.crm_sales_details
where sls_order_dt <= 0;

select 
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 
or len(sls_order_dt) != 8
or sls_order_dt > 20200101 
or sls_order_dt < 19000101;


select 
nullif(sls_ship_dt,0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 
or len(sls_ship_dt) != 8
or sls_ship_dt > 20200101 
or sls_ship_dt < 19000101;

select 
nullif(sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 
or len(sls_due_dt) != 8
or sls_due_dt > 20200101 
or sls_due_dt < 19000101;

select
sls_order_dt 
from bronze.crm_sales_details

-- check for Invalid date orders 
select
* from 
bronze.crm_sales_details
where sls_order_dt > sls_ship_dt 
or sls_order_dt > sls_due_dt;

-- check data consistency : between sales,quantity, and price
-- >> sales = quantity * price 
-- >> values must not be null,zero or negative

select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case 
	when sls_sales is null   or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales,
case 
	when sls_price is null or sls_price <= 0
		then sls_sales / nullif(sls_quantity,0)
	else sls_price
end  as sls_price
from bronze.crm_sales_details
where sls_sales !=  sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0 
order by sls_sales,sls_quantity, sls_price ;
 

select distinct
sls_sales ,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales !=  sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0 
order by sls_sales,sls_quantity, sls_price ;
---------------------------------------
-- Checking : silver.crm_sales_details
---------------------------------------
select distinct
sls_sales ,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales !=  sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0 
order by sls_sales,sls_quantity, sls_price ;


select
*
from  silver.crm_sales_details;

-- ==========================================================
-- checking for bronze.erp_cust_az12 and silver.erp_cust_az12
-- ==========================================================

--------------------------------------
-- checking : bronze.erp_cust_az12
--------------------------------------
-- check the cid with silver.crm_cust_info
select 
case 
	when cid like 'NAS%' then SUBSTRING (cid,4,len(cid))
	else cid
end as cid,
case when  bdate > getdate() then null
	else bdate
end as bdate,
gen
from  bronze.erp_cust_az12 
where case when cid like 'NAS%' then SUBSTRING (cid,4,len(cid))
		else cid
end not in (select distinct cst_key from silver.crm_cust_info);

-- Identify out-of-range dates

select distinct
bdate 
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate();

-- Data standardization & consistency
select distinct gen ,
case when upper(trim(gen)) in ('M','MALE') then 'Male'
	when upper(trim(gen)) in ('F','FEMALE') then 'Female'
	else  'n/a'
end as gen
from bronze.erp_cust_az12
----------------------------------
-- check : silver.erp_cust_az12
----------------------------------
select distinct
bdate 
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate();

-- ========================================================
-- checking for bronze.erp_loc_a101 and silver.erp_loc_a101
-- ========================================================

----------------------------------
-- checking : bronze.erp_loc_a101 
----------------------------------
select 
replace(cid,'-','') cid,
cntry
from bronze.erp_loc_a101;

-- Data Standardization & Consistency 
select distinct cntry 
from bronze.erp_loc_a101;


select distinct cntry as ols_cntry,
case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US','USA') then 'United States'
	when trim(cntry) = '' or cntry is null then 'n/a'
	else trim(cntry)
end cntry
from bronze.erp_loc_a101
order by cntry;
----------------------------------
-- checking : silver.erp_loc_a101
----------------------------------
select distinct cntry 
from silver.erp_loc_a101
order by cntry;

-- ===============================================================
-- checking for  bronze.erp_px_cat_g1v2 and silver.erp_px_cat_g1v2
-- ================================================================

-------------------------------------
-- checking : bronze.erp_px_cat_g1v2
-------------------------------------
select id,
cat,
subcat,
maintenance
from
bronze.erp_px_cat_g1v2
where id!=trim(id) or cat!=trim(cat) or subcat!=trim(subcat) or maintenance!=trim(maintenance);

-- checking for duplicates

select 
distinct cat
from
bronze.erp_px_cat_g1v2;

select 
distinct subcat
from
bronze.erp_px_cat_g1v2;

select 
distinct maintenance
from
bronze.erp_px_cat_g1v2
-------------------------------------
-- checking : silver.erp_px_cat_g1v2
-------------------------------------
select id,
cat,
subcat,
maintenance
from
silver.erp_px_cat_g1v2
where id!=trim(id) or cat!=trim(cat) or subcat!=trim(subcat) or maintenance!=trim(maintenance);
