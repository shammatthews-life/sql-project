/*
In this table the data is by default has no issue so view is created Right away.
*/
--***********************************
--steps to create view for salesdate
--***********************************

-- gather the necessary columns
go
go
if OBJECT_ID('gold.fact_sales') is not null
drop view gold.fact_sales;
go
create view gold.fact_sales as
select 
sd.sls_ord_num as order_number,      --*****************************  
pr.product_key as product_key,       --  keys from all three table *
cs.customer_key as customer_key,     --*****************************
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,     -- all date 
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,         --*****************
sd.sls_quantity as quantity ,         -- amount details *
sd.sls_price as price                 --*****************
from silver.crm_sales_info as sd
left join gold.dim_products as pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers as cs
on sd.sls_cust_id=cs.customer_id


go
-- checking the view 


go
select * from gold.fact_sales -- no issue same data 
select * from gold.fact_sales f
left join     gold.dim_customers as c
on c.customer_key=f.customer_key
left join     gold.dim_products as p
on f.product_key=p.product_key
where c.customer_key is  null or p.product_key is null  -- to find whether we have any unmatched data
go                        
