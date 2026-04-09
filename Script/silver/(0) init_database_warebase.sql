/*
create table in the new scheme name silver as bronze layer is finished 
along with dwh_create_date with datetim datatype which has getdate which is by default 
*/
use warehouse;

if OBJECT_ID('silver.crm_cust_info','u') is not null
drop table silver.crm_cust_info;
create table silver.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(100),
cst_lastname nvarchar(70),
cst_marital_status nvarchar(100),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime default getdate()
);


  if OBJECT_ID('silver.crm_prd_info','u') is not null
  drop table silver.crm_prd_info;
  create table silver.crm_prd_info(
  prd_id int,
  prd_key nvarchar(50),
  prd_nm nvarchar(50),
  prd_cost int ,
  prd_line nvarchar(50),
  prd_start_dt datetime,
  prd_end_dt datetime ,
  dwh_create_date datetime default getdate()

  );


  if OBJECT_ID('silver.crm_sales_info','u') is not null
  drop table silver.crm_sales_info;
  create table silver.crm_sales_info(
  sls_ord_num nvarchar(50),
  sls_prd_key nvarchar(50),
  sls_cust_id int,
  sls_order_dt int,
  sls_ship_dt int,
  sls_due_dt int,
  sls_sales int,
  sls_quantity int,
  sls_price int ,
  dwh_create_date datetime default getdate()

  );


  if OBJECT_ID('silver.erp_loc_a101','u') is not null
  drop table silver.erp_loc_a101;
  create table silver.erp_loc_a101(
  cid nvarchar(50),
  cntry nvarchar(50),
  dwh_create_date datetime default getdate()

  );


  if OBJECT_ID('silver.erp_cust_az12','u') is not null
  drop table silver.erp_cust_az12;
  create table silver.erp_cust_az12(
  cid nvarchar(50),
  bdate date,
  gen nvarchar(50),
  dwh_create_date datetime default getdate()

  );


  if OBJECT_ID('silver.erp_px_cat_g1v2','u') is not null
  drop table silver.erp_px_cat_g1v2;
  create table silver.erp_px_cat_g1v2(
  id nvarchar(50),
  cat nvarchar(50),
  subcat nvarchar(50),
  maintenance nvarchar(50) ,
  dwh_create_date datetime default getdate()
);

