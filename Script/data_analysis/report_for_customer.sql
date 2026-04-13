
--***********************
--build customer report *
--***********************

/*
  1. gather customer name,ages,transaction details 
  2.segmentation customer into (vip,regular,new) and age group
  3. aggregate customer-level metrics:
      -- total order
      -- total sales
      -- total quantity
      --total product
      -- lifespan (in month)
  4. kpi:
       -- recent order (month)
       -- average order value
       -- average monthly spend
       */


--************************************** 
-- step 1 retrieve all the core column *
--**************************************
;
go
create view gold.customer_report as
with customer_base_query
as  (
select
f.order_number
,f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
customer_number,
CONCAT(c.first_name,' ',c.last_name) as customer_name,
datediff(year,c.birthdate,getdate()) as dob
from gold.fact_sales as f
left join gold.dim_customers as c
on f.customer_key=c.customer_key
where order_date is not null
)
--**********************
-- customer aggeration *
--**********************
,customer_aggregation as(
select
customer_key,customer_name,customer_number,dob,
count(distinct(order_number)) as total_order,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct(product_key)) as product_key,
max(order_date) as first_order ,
min(order_date) as recent_order,
DATEDIFF(month , min(order_date),max(order_date)) as lifespan
from customer_base_query
group by customer_key,customer_name,customer_number,dob
)
select
customer_key,customer_name,customer_number,
case when dob <20 then 'under 20'
     when dob between 20 and 30 then '20-30'
     when dob between 30 and 40 then '30-40'
     when dob between 40 and 50 then '40-50'
     else 'above 50'
     end as age_group,
case when lifespan >=12 and total_sales>=5000 then 'vip'
     when lifespan >= 12  and total_sales<5000 then 'regular'
     else 'new'
     end as cutom_group,
DATEDIFF(month,recent_order,getdate()) as recency,
total_order,total_quantity,total_sales
-- average order value
, case when total_sales =0 or total_sales is null then 0
       else total_sales/total_order
       end as avg_order_value
, case when lifespan =0 or lifespan is null then 0
       else total_sales/lifespan 
       end as avg_monthly_spend

from
customer_aggregation
go

  
-- after finishing the report always convert the report into a view 


select *
from gold.customer_report

