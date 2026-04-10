/*

in this code we have created a view for customer information by connecting 3 table informations together 

*/



use warehouse;
-- start by gathering all the customer inforations


select 
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
ci.cst_marital_status as martialstatus,
ci.cst_create_date as create_date,
ci.cst_gndr,
ca.bdate as birthdate,
ca.gen ,
la.cntry as country
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on   ci.cst_key =ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key =la.cid






--****************
-- filtering data
--***************

--filterdata with one genre 
--crm data has first priority 


select distinct ci.cst_gndr,ca.gen,
case when ci.cst_gndr !='unknown' then ci.cst_gndr
     else coalesce(ca.gen,'unknown')
     end
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on   ci.cst_key =ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key =la.cid
order by 1,2



--********************
-- checking the data *
--********************



-- checking any dublicate values from the retived table 

select cst_id , count(*) from (
select 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on   ci.cst_key =ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key =la.cid)t 
group by cst_id
having count(*) >1  -- no issue fould there is no dublicate



--******************
-- finalised table view *
--******************


go -- creating view so that we can use the query anytime without retyping
if OBJECT_ID('gold.dim_customers') is not null
drop view gold.dim_customers;
go
go
create view gold.dim_customers as
select 
row_number() over(order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
ci.cst_marital_status as martialstatus,case when ci.cst_gndr !='unknown' then ci.cst_gndr
     else coalesce(ca.gen,'unknown')
     end  gender ,
ca.bdate as birthdate,
ci.cst_create_date as create_date,
la.cntry as country
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on   ci.cst_key =ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key =la.cid
go

-- checking  
select distinct gender from gold.dim_customers -- no issue found
