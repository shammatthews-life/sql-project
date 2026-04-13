
--*******************
-- analysis process *
--*******************

--******************
--change over time *
--******************
-- done by using agregate function with time dimension


select 
year(order_date) as year_o,
sum(sales_amount) as total,
count(distinct(customer_key)) as total_customer,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date)
order by year(order_date) asc

select 
format(order_date,'yyy-MMM') as year_o,
sum(sales_amount) as total,
count(distinct(customer_key)) as total_customer,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by format(order_date,'yyy-MMM') 
order by format(order_date,'yyy-MMM')  asc

-- total sales per month and running total of sales over time
select 
month_sales,
total_sales,
sum(total_sales)  over(order by month_sales) as running_total,
sum(avg_sales)  over(order by month_sales) as running_avg
from(
select
datetrunc(month,order_date) as month_sales,
sum(sales_amount) as total_sales,
avg(sales_amount) as avg_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
)t

-- analysisng yearly performance of the product by comapring with sales
with yearly_product_sales as(
    select 
    year(f.order_date) as order_year,
    p.product_name,
    sum(f.sales_amount) as total_sales
    from gold.fact_sales as f
    left join gold.dim_products as p
    on f.product_key=p.product_key
    where year(f.order_date) is not null 
    group by year(f.order_date),product_name
)

select
order_year,
product_name,
total_sales,
avg(total_sales) over(partition by product_name) as avg_sales,
case when total_sales - avg(total_sales) over(partition by product_name) <0 then 'below_avg'
     when total_sales -  avg(total_sales) over(partition by product_name)>0 then 'above_avg'
     else 'on_avg'
     end as avg_rank,
lag(total_sales) over(partition by product_name order by order_year) as previous_year_sales,
total_sales-lag(total_sales) over(partition by product_name order by order_year) as diff_previous_current,
case when total_sales-lag(total_sales) over(partition by product_name order by order_year) >0 then'increasing'
     when total_sales-lag(total_sales) over(partition by product_name order by order_year)<0  then 'decreasing'
          else 'nochange'
          end as product_sales_change
from yearly_product_sales
order by product_name,order_year





Part to whole analysis
[measure]/total[measure] * 100


with categery_sales as (
select
category,
sum (sales_amount) as total_sales
from gold.fact_sales as f
left join gold.dim_products as p
on f.product_key =p.product_key
group by category
) 
select * , sum(total_sales) over() as over_all_sales,
concat(round((cast(total_sales as float)/sum(total_sales) over() )*100,2),'%') as total_percentage
from categery_sales
order by total_sales desc


--********************
-- data segmentation *
--********************

-- segment product with cost range 
with product_segment as (
select product_key,product_name,cost,
case when cost <500 then 'below 500'
     when cost between 500 and 1000 then '500-1000'
     when cost between 1000 and 1500 then '1000-1500'
     else 'above 1000'
     end as cost_range
from gold.dim_products
)
select cost_range 
, count(product_key) as total 
from product_segment
group by cost_range
order by total desc


--****************************
-- segment based on customer *
--****************************


-- vip 12 month spend 5000 and regular 12 spend less 5000 and new who register less than 12 month

with customer_segemtation as(
select c.customer_key,
sum(f.sales_amount) as total_amount,
min(f.order_date) as first_order ,
max(f.order_date) as last_order,
DATEDIFF( month,min(f.order_date),max(f.order_date)) as date_differnece
from gold.dim_customers as c
left join gold.fact_sales as f
on f.customer_key = c.customer_key
group by c.customer_key
)
select 
 case when date_differnece >= 12 and total_amount >5000 then 'vip'
                   when date_differnece >= 12 and total_amount <5000 then 'regular'
                   when date_differnece <12 then 'new'
                   else 'new'
                   end as customer_categery,
count(customer_key) as total_customer
from customer_segemtation
group by case when date_differnece >= 12 and total_amount >5000 then 'vip'
                   when date_differnece >= 12 and total_amount <5000 then 'regular'
                   when date_differnece <12 then 'new'
                   else 'new'
                   end 
