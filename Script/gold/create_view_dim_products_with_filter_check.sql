/*
In this file dim_product view is created and tested and filter with the necessary functions
*/




--********************************************
-- steps to creating view for product values *
--********************************************
-- gather all the necessary table
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pc.id=pn.cat_id
where pn.prd_end_dt is null


--***************************************
-- filter and checking the table values *
--***************************************

--********************************
-- product key dublicate check   *
--********************************

select prd_key,count(*) from (
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pc.id=pn.cat_id
where pn.prd_end_dt is null)t group by prd_key having count(*)>1
-- no issue found there is no dublicate

-- finalised table 
go
if OBJECT_ID('gold.dim_products') is not null
drop view gold.dim_products;
go
go
create view gold.dim_products as
select 
row_number() over(order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance as maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as [start_date]
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pc.id=pn.cat_id
where pn.prd_end_dt is null -- to filter out the latest data
go


--********************
-- checking the view *
--********************
select * from gold.dim_products -- no issue found
