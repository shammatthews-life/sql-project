

/*
second report for products
  1. include product name,category,subcategory,cost
  2. segments product with high-performance,lowperformance,mid-performance,
  3.  aggerate 
      -- total order
      -- total sales
      -- total quality sold
      -- toal customers 
      -- lifespan
 4. calculate kpi 
      -- recency
      -- average order revenue
      -- average monthly revenue
*/

-- gather all the required columns
go
create view gold.product_report as
with product_base
as(
select 
p.product_id,
p.product_name,
p.product_number,
p.category,
p.subcategory,
f.customer_key,
p.cost,
datediff(month,p.start_date,GETDATE()) as expire,
f.order_date,
f.order_number,
f.sales_amount,
f.quantity,
f.product_key
from gold.fact_sales as f
left join gold.dim_products as p
on f.product_key=p.product_key
where f.order_date is not null
)
-- performin the aggregated functions 
, product_aggregation_function as(
select
product_id,
product_name,
cost,
category,
subcategory,
expire as product_age,
count(distinct(order_number)) as total_order,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct(customer_key)) as total_customer_brought,
max(order_date) as recent_order,
datediff(month,min(order_date),max(order_date)) as lifespan,
datediff(month,max(order_date),GETDATE())as recency,
round(avg(cast(sales_amount as float) / nullif(quantity,0)),2) as avg_selling_price
from product_base
group by product_id,product_name,cost,category,subcategory,expire
)
-- finalised table values
select 
product_id,product_name,cost,category,subcategory,product_age,total_sales,total_order,total_quantity  ,recent_order,
case when lifespan >=12 and total_quantity >1000 then 'high performance'
     when lifespan >=12 and total_quantity between 500 and 1000 then 'mid performance'
     else 'low performance'
     end as product_performance_life_quantity,recency,
case when total_order <=0 and total_order is null then 0
     else total_sales/total_order
     end as avg_order_value,
case when lifespan <=0 and lifespan is null then 0
     else total_sales/ lifespan
     end as avg_revenu_month,total_customer_brought,
case when total_sales >50000 then 'high sales'
     when total_sales >= 25000     then 'avg sales '
     else 'low-sales'
     end sales_performance
from 
product_aggregation_function
go

select * from gold.product_report
