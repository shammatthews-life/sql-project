-- this file has the steps involved and detailed explanation for each process in filtering bronze schema tables and updating silver schema tables
-- silver layer
-- analyse the table -> clean the table -> check the data -> document the values
--  crm-sales has customer id and product id can be used to connect the other two tables
-- in erp cust we have customerkey and in cust_info of crm we have the key and the same key n erp_loc_a101 
-- product key from crm_prd_info and product id of erp_px_cat_g1v2

-- to alter and modify data we create  a new table with schema silver 

-- step 2 use warehouse

-- check data if it has dublicate 
select cst_id,count(*) as c from [bronze].[crm_cust_info] group by cst_id having count(*)>1
-- we have dublicates
-- to remove dublicates
select * from (
select *,ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) as lastfag from bronze.crm_cust_info where cst_id is not null 
)t where lastfag=1 
-- distinct value alone is selected based on lastest created time

-- to trim extra space in string
select cst_firstname from bronze.crm_cust_info where cst_firstname != trim(cst_firstname)
-- we have extra space values in first & lastname
-- to clean the extra space 



--***********************
--cleaning the first table
--************************
select 
cst_id,cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_marital_status))='S' then 'Single'
     when upper(trim(cst_marital_status) )='M' then 'Married'
     else 'unknown'
end cst_marital_status ,
case when upper(trim(cst_gndr))='F' then 'Female'
     when upper(trim(cst_gndr) )='M' then 'Male'
     else 'unknown'
end cst_gndr,cst_create_date
from (
select *,ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) as lastfag from bronze.crm_cust_info where cst_id is not null 
)t where lastfag=1 


--****************************************************************
-- to insert the filtered value into silver as corrected values
--***************************************************************
truncate table silver.crm_cust_info;
insert into silver.crm_cust_info (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
select 
cst_id,cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_marital_status))='S' then 'Single'
     when upper(trim(cst_marital_status) )='M' then 'Married'
     else 'unknown'
end cst_marital_status ,
case when upper(trim(cst_gndr))='F' then 'Female'
     when upper(trim(cst_gndr) )='M' then 'Male'
     else 'unknown'
end cst_gndr,cst_create_date
from (
select *,ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) as lastfag from bronze.crm_cust_info where cst_id is not null 
)t where lastfag=1 



 


-- check silver data is perfect or not
select ( cst_id),count(*)  from silver.crm_cust_info group by cst_id having COUNT(*) >1
select cst_firstname,cst_lastname from silver.crm_cust_info where cst_firstname != trim(cst_firstname) or cst_lastname != trim(cst_lastname)
select distinct(cst_marital_status) from silver.crm_cust_info



--***************************
--table 2 product info
--****************************



-- moving to the next table bronze.crm_prd_info

select prd_id,count(*) from bronze.crm_prd_info group by prd_id having count(*) >1 
-- predid is clean there is no dublicate and null 
select prd_nm from bronze.crm_prd_info where prd_nm!=trim(prd_nm) -- there is no empty space it is good 
select prd_cost from bronze.crm_prd_info where prd_cost <1 or prd_cost is null -- we have nulll values
select distinct(prd_line) from bronze.crm_prd_info -- we have null
select * from bronze.crm_prd_info where prd_start_dt>prd_end_dt -- we have wrond dates


-- spliting the prd_key to create catagory_id for connecting tables
select distinct id from bronze.erp_px_cat_g1v2 -- format ac_bc we have _ but in prd_info we have -


--******************************
--       cleaning table 2
--****************************
select 
prd_id,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,len(prd_key) ) as prd_key,
prd_nm,
isnull(prd_cost,0)as prd_cost,
case when upper(Trim(prd_line)) ='M' then 'Mountain'
     when upper(trim(prd_line)) ='R' then 'Road'
     when upper(trim(prd_line)) ='S' then 'Other Sales'
     when upper(trim(prd_line))='T' then 'Touring'
     else 'unknown'
  end as prd_line,
cast(prd_start_dt as date) as prd_start_dt ,
cast(lead (prd_start_dt) over(partition by prd_key order by prd_start_dt ) -1 as date)as prd_end_dt
from bronze.crm_prd_info





--******************************
-- inserting table 2 in silver
--*******************************

truncate table silver.crm_prd_info;
insert into silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt 
)
select 
prd_id,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,len(prd_key) ) as prd_key,
prd_nm,
isnull(prd_cost,0)as prd_cost,
case when upper(Trim(prd_line)) ='M' then 'Mountain'
     when upper(trim(prd_line)) ='R' then 'Road'
     when upper(trim(prd_line)) ='S' then 'Other Sales'
     when upper(trim(prd_line))='T' then 'Touring'
     else 'unknown'
  end as prd_line,
cast(prd_start_dt as date) as prd_start_dt ,
cast(lead (prd_start_dt) over(partition by prd_key order by prd_start_dt ) -1 as date)as prd_end_dt
from bronze.crm_prd_info



--********************************************
--check the quality of the silver product table
--*******************************************


select prd_id,COUNT(*) from silver.crm_prd_info group by prd_id having count(*) >1 or prd_id is null  
-- no dublicate found

select prd_cost from silver.crm_prd_info where prd_cost<0 and prd_cost is null
-- data has no flas

select distinct(prd_line) from silver.crm_prd_info
-- null  modified to unknow no error or mistakes


select * from silver.crm_prd_info where prd_start_dt > prd_end_dt
-- every date is added in the correct order 





--- *******************************************
--- third table sales table (crm_sales_details)
--- *******************************************


--********************************
--checking table rows & coonection
--*********************************

--**********
--connection
--***********


-- to check product key and customer key from silver table and sales table for connectio
select sls_prd_key from bronze.crm_sales_info where sls_prd_key  not in (select prd_key from silver.crm_prd_info)
-- there is no prd_key left over sutable to connect two tables

-- to check the customer key from silver customer table and bronze sales table 
select sls_cust_id  from bronze.crm_sales_info where sls_cust_id not in (select cst_id from silver.crm_cust_info)
-- there is no customer id left over sutable to connect two table 


--******************
-- checking each row 
--******************

select sls_order_dt from bronze.crm_sales_info 
where sls_order_dt<0 or -- issue found
len(sls_order_dt) <8 or len(sls_order_dt) >8 -- issue found
or sls_order_dt > 20260911 or sls_order_dt <19990187  -- no issue found 
-- need to convert from integer to date and it also have value 0 and checking whether the date are above and below the company run time 



-- same checking with shipping date 

select sls_ship_dt from bronze.crm_sales_info 
where sls_ship_dt<0 or -- no issue found
len(sls_ship_dt) <8 or len(sls_ship_dt) >8 -- no issue found
or sls_ship_dt > 20260911 or sls_ship_dt <19990187  -- no issue found 
-- column data is perfect need to change the integer to date 

-- same test in due date 
select sls_due_dt from bronze.crm_sales_info 
where sls_due_dt<0 or -- no issue found
len(sls_due_dt) <8 or len(sls_due_dt) >8 -- no issue found
or sls_due_dt > 20260911 or sls_due_dt <19990187  -- no issue found 
-- no issue found only type conversion 

-- test order date must be small than due and shiping date 
select *
from bronze.crm_sales_info
where sls_order_dt > sls_ship_dt or
      sls_order_dt >sls_due_dt
 -- no issue found order date is small than shipping and due date

 /* bussiness rule 
       sales = quantity * price 
       negative,zero,nulls not allowed 
 
 */
 select distinct sls_sales,sls_quantity,sls_price 
 from bronze.crm_sales_info 
 where sls_sales != sls_quantity*sls_price                            --issue found
  or sls_sales is null or sls_quantity is null or sls_price is null   -- issue found in price,sales
  or sls_sales <=0 or sls_quantity <=0 or sls_price <=0 -- issue found in sales,price
  order by sls_sales,sls_quantity,sls_price 
  -- task need to do sales is negative,zero,null then use quantity and price to fix it
  -- task price is zero or null , calculate using sales and quantity
  -- price is negative , convert it to a positive value 



  -- to test the solution
  -- to check the result before inserting 
 select
 case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price) then sls_quantity * abs(sls_price)
     else sls_sales
     end  as  sls_sales,
  sls_quantity,
  case when sls_price <=0 or sls_price is null then abs(sls_sales)/nullif(sls_quantity,0)
     else sls_price 
     end as sls_price
 from
     bronze.crm_sales_info
  where sls_sales != sls_quantity*sls_price                            --no issue found
  or sls_sales is null or sls_quantity is null or sls_price is null   -- no issue found in price,sales
  or sls_sales <=0 or sls_quantity <=0 or sls_price <=0 --no  issue found in sales,price
  order by sls_sales,sls_quantity,sls_price 
 


     --**************************
     --   cleaning table 3
     --**********************

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 then  null
     when len(sls_order_dt)!=8 then null
     else cast(cast(sls_order_dt as Varchar(8)) as date)
     end sls_order_dt,                                      --****************************************
case when sls_ship_dt = 0 then  null                        -- just in case for future purpose
     when len(sls_ship_dt)!=8 then null                     -- case is added for both ship and due date 
     else cast(cast(sls_ship_dt as Varchar(8)) as date)     --************************************
     end sls_ship_dt,
case when sls_due_dt = 0 then  null
     when len(sls_due_dt)!=8 then null
     else cast(cast(sls_due_dt as Varchar(8)) as date)
     end sls_due_dt,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price) then sls_quantity * abs(sls_price)
     else sls_sales
     end  as  sls_sales,
sls_quantity, -- no issue in quantity 
case when sls_price <=0 or sls_price is null then abs(sls_sales)/nullif(sls_quantity,0)
 else sls_price end as sls_price
from bronze.crm_sales_info



--*****************************************************
-- inserting cleaned data into the silvercrm_sales_info
--******************************************************
truncate table silver.crm_sales_info;
insert into silver.crm_sales_info (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 then  null
     when len(sls_order_dt)!=8 then null
     else cast(cast(sls_order_dt as Varchar(8)) as date)
     end sls_order_dt,                                      --****************************************
case when sls_ship_dt = 0 then  null                        -- just in case for future purpose
     when len(sls_ship_dt)!=8 then null                     -- case is added for both ship and due date 
     else cast(cast(sls_ship_dt as Varchar(8)) as date)     --************************************
     end sls_ship_dt,
case when sls_due_dt = 0 then  null
     when len(sls_due_dt)!=8 then null
     else cast(cast(sls_due_dt as Varchar(8)) as date)
     end sls_due_dt,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price) then sls_quantity * abs(sls_price)
     else sls_sales
     end  as  sls_sales,
sls_quantity, -- no issue in quantity 
case when sls_price <=0 or sls_price is null then abs(sls_sales)/nullif(sls_quantity,0)
 else sls_price end as sls_price
from bronze.crm_sales_info


--****************************************
--- quality check of silver.crm_sales_info
--*****************************************
 select
 sls_sales,
  sls_quantity,
   sls_price
 from
     silver.crm_sales_info
  where sls_sales != sls_quantity*sls_price                            --no issue found
  or sls_sales is null or sls_quantity is null or sls_price is null   -- no issue found in price,sales
  or sls_sales <=0 or sls_quantity <=0 or sls_price <=0 --no  issue found in sales,price
  order by sls_sales,sls_quantity,sls_price 
  -- no issue found with sales,quality,price


  select * from silver.crm_sales_info -- no issue found date datatype everything is done









  --**************************************
  -- clean and load 4th table erp_cust_az12
  --****************************************

  select cid,bdate,gen from bronze.erp_cust_az12
   select * from silver.crm_cust_info 
  --************************
  -- connection stablisation 
  --*************************
  -- problem 
  select cid from bronze.erp_cust_az12 -- issue found we have nas at the beginning

  --solution 
  select
  case when cid like 'NAS%' then  substring(cid,4,len(cid))
  else cid 
  end as cid
  from bronze.erp_cust_az12
 
 -- problem
 select bdate
 from bronze.erp_cust_az12
 where bdate<'1923-01-01' or bdate > getdate() -- issue found bdate out in future and customer older than 100 years 
 -- solution neglect old customer
 select
  case when bdate > getdate() then cast(getdate() as date)
 else bdate end as bdate from bronze.erp_cust_az12 order by bdate desc

 -- problem in  gender
 select distinct(gen) from bronze.erp_cust_az12 -- issue found 5 different values

 -- solution 
 select case when upper(trim(gen)) in ('FEMALE','F') then 'Female'
             when upper(trim(gen)) in ('MALE','M')   then 'Male'
             else 'unknown'
             end as gen
   from bronze.erp_cust_az12


 --***************
 -- data cleaning
 --****************

  select 

    case when cid like 'NAS%' then  substring(cid,4,len(cid))
    else cid 
    end as cid,

   case when bdate > GETDATE() then cast(getdate() as date)
         else bdate
         end as bdate,
   case when upper(trim(gen)) in ('FEMALE','F') then 'Female'
             when upper(trim(gen)) in ('MALE','M')   then 'Male'
             else 'unknown'
             end as gen
   from bronze.erp_cust_az12




 --***************
 -- data insertion
 --****************

 truncate table silver.erp_cust_az12
 insert into silver.erp_cust_az12(cid,bdate,gen)
  select 

    case when cid like 'NAS%' then  substring(cid,4,len(cid))
    else cid 
    end as cid,

   case when bdate > GETDATE() then cast(getdate() as date)
         else bdate
         end as bdate,
   case when upper(trim(gen)) in ('FEMALE','F') then 'Female'
             when upper(trim(gen)) in ('MALE','M')   then 'Male'
             else 'unknown'
             end as gen
   from bronze.erp_cust_az12

   --***************************
   ---checking the data quality 
   ---**************************

   select bdate from silver.erp_cust_az12 order by bdate desc -- no issue found
   select cid from silver.erp_cust_az12 -- no issue found
   select distinct(gen) from silver.erp_cust_az12 -- no issue found
   select * from silver.erp_cust_az12 -- no issue found







---*********************************************************
--- cleanning and loading the next table silver.erp_loc_a101
----*********************************************************


--************************
-- connection stabilisation
---************************

select * from bronze.erp_loc_a101
select * from silver.crm_cust_info  -- issue found need to remove -

-- solution
select replace(cid,'-','') as cid 
from bronze.erp_loc_a101

--*************************
-- problem identification *
--*************************

select replace(cid,'-','') as cid 
from bronze.erp_loc_a101 where replace(cid,'-','') not in (select cst_key from silver.crm_cust_info )
-- no issue found every cid from both table has matched

select distinct(cntry) from bronze.erp_loc_a101 -- issue found
-- solution 
select distinct(
case when upper(Trim(cntry)) ='DE' then 'Germany'
     when upper(Trim(cntry)) in ('US','USA') then 'United States'
     when trim(cntry)='' or cntry is null then 'Unknown'
     else cntry
     end) as cntry
from bronze.erp_loc_a101





--****************************************
--          data cleaning
--****************************************

select
replace(cid,'-','') as cid ,
case when upper(Trim(cntry)) ='DE' then 'Germany'
     when upper(Trim(cntry)) in ('US','USA') then 'United States'
     when trim(cntry)='' or cntry is null then 'Unknown'
     else cntry
     end as cntry
from bronze.erp_loc_a101



--***************************************
--data insertion into silver.erp_loc_a101
--***************************************
truncate table silver.erp_loc_a101
insert into silver.erp_loc_a101(cid,cntry)
select
replace(cid,'-','') as cid ,
case when upper(Trim(cntry)) ='DE' then 'Germany'
     when upper(Trim(cntry)) in ('US','USA') then 'United States'
     when trim(cntry)='' or cntry is null then 'Unknown'
     else cntry
     end as cntry
from bronze.erp_loc_a101


--******************************
-- check the silver.erp_loc_a101
--*******************************

select * from silver.erp_loc_a101
select distinct(cntry) from silver.erp_loc_a101
-- no issue found

--***********************************************************
-- cleaning and loading the 6th table bronze.erp_px_cat_g1v2
--************************************************************
-- connection mainenance *
-- ***********************
select id from bronze.erp_px_cat_g1v2 where id not in (
select cat_id from silver.crm_prd_info)
-- only one id which is not available co_pd


--*******************
-- check for problems
--*******************

select * from bronze.erp_px_cat_g1v2
where cat != trim (cat) or subcat != trim(subcat)or maintenance != trim(maintenance)
-- no issue found the values have no unwanted space

select distinct(cat) from bronze.erp_px_cat_g1v2  -- no issue found
select distinct(subcat) from bronze.erp_px_cat_g1v2  -- no issue found
select distinct(maintenance) from bronze.erp_px_cat_g1v2  -- no issue found



--******************************************************
-- loading date directly as there is no change required
--********************************************************
truncate table silver.erp_px_cat_g1v2 
insert into silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2

--**************
-- check the data
--******************

select * from silver.erp_px_cat_g1v2
-- no issue found



