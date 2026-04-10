/*

need to explore and find all datatype and to find the dimension of the table 
to deterime the bussines stategies and to get an idea which has sold well and low 
to know the low, max of all columns that are necessary 

*/



-- explore all object column  in the database
select * from INFORMATION_SCHEMA.TABLES

select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='dim_customers';

-- explore all country,catagories of products
go
use datawarehouse;

select distinct(country) from [gold].[dim_customers]
select distinct category,subcategory from gold.dim_products
go
-- explore the boudaries
select 
min(order_date) as first_order ,
max(order_date) as last_order
from gold.fact_sales

-- explore the boudaries of age 
select 
datediff(year,min(birthdate),GETDATE()) as older,
datediff(year,max(birthdate),GETDATE()) as young
from gold.dim_customers

-- boundaries of agregate funtion exploration
select sum(sales_amount) as total_sales from gold.fact_sales       -- totall revenue
select sum(quantity) as total_sales from gold.fact_sales           -- no of product sold
select avg(sales_amount) as total_sales from gold.fact_sales        --avg sales amount
select count(distinct(order_number)) as total_sales from gold.fact_sales  -- no of orders

select count(customer_key) as total from gold.dim_customers;
select count(distinct(customer_key)) as orderedcustomer from gold.fact_sales;

--generating report for all measures 
-- key metrices
select 'total sales ' as measure_name ,sum(sales_amount) as measure_values from gold.fact_sales 
union all 
select 'total quantity ',sum(quantity)  from gold.fact_sales  
union all 
select 'avg sales amount ', avg(sales_amount)  from gold.fact_sales        --avg sales amount
union all 
select 'total order',count(distinct(order_number))  from gold.fact_sales  -- no of orders
union all
select 'total customer' ,count(customer_key)  from gold.dim_customers
union all
select 'customer who has placed order', count(distinct(customer_key))  from gold.fact_sales;

-- magnitude analysis
 

 -- customer based on country
 select country,count(customer_key) as total_customers
 from gold.dim_customers
 group by country 
 order by count(customer_key)
 -- customer by gender 
  select gender,count(customer_key) as total_customers
 from gold.dim_customers
 group by gender 
 order by count(customer_key)
 -- product by catagory
 select category,count(product_key) as total_customers
 from gold.dim_products
 group by category 
 order by count(product_key)
 --avg cost in each category
select category,avg(cost)
from gold.dim_products
group by category

-- most spending customer
select
c.customer_key,
c.last_name,
c.first_name,
sum(f.sales_amount)
from [gold].[fact_sales] as f
left join [gold].[dim_customers] as c
on c.customer_key = f.customer_key
group by c.customer_key,c.last_name,
c.first_name
order by sum(f.sales_amount) desc

--rank analysis
-- top 5 customers 
select top 5
c.customer_key,
c.last_name,
c.first_name,
sum(f.sales_amount)
from [gold].[fact_sales] as f
left join [gold].[dim_customers] as c
on c.customer_key = f.customer_key
group by c.customer_key,c.last_name,
c.first_name
order by sum(f.sales_amount) desc


--5 least buy customers
select top 5
c.customer_key,
c.last_name,
c.first_name,
sum(f.sales_amount)
from [gold].[fact_sales] as f
left join [gold].[dim_customers] as c
on c.customer_key = f.customer_key
group by c.customer_key,c.last_name,
c.first_name
order by sum(f.sales_amount) asc




