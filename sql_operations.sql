select * from df_orders limit 10;

-- # find top 10 highest revenue generating products
select product_id, sum(sale_price) as sales from df_orders group by product_id order by sales desc limit 10;

-- # find top 5 highest selling products in each region
with cte as(
    select region, 
    product_id, 
    sum(sale_price) as sales 
    from df_orders 
    group by region, product_id) 
    select * from 
    (select *, 
    row_number() over(partition by region order by sales desc) as rn
     from cte) A 
     where rn<=5;

-- # find month over month comparison for 2022 and 2023 sales
with cte as (
    select Year(order_date) as year, 
    MonthName(order_date) as month, 
    sum(sale_price) as sales 
    from df_orders 
    group by year, month) 
    select month, 
    sum(case when year=2022 then sales else 0 end) as sales_2022, 
    sum(case when year=2023 then sales else 0 end) as sales_2023 
    from cte 
    group by month 
    order by month;

-- # for each category, which month has highest sales
with cte as (
    select category, 
    year(order_date) as year, 
    MonthName(order_date) as month, 
    sum(sale_price) as sales 
    from df_orders 
    group by category, 
    year, 
    month 
    order by year, 
    month) 
    select category, 
    year, 
    month, 
    sales 
    from (
        select *, 
        row_number() over(partition by category order by sales desc) as rn 
        from cte) cte2 
        where rn<=1; 

-- # which sub category had highest growth by profit in 2023 compare to 2022
with cte as (
    select sub_category, 
    year(order_date) as year, 
    sum(profit) as profit 
    from df_orders 
    group by sub_category, 
    year order by sub_category) 
    select sub_category, 
    ((profit_2023-profit_2022)*100)/profit_2022 as profit 
    from(
        select sub_category, 
        sum(case when year=2022 then profit else 0 end) as profit_2022, 
        sum(case when year=2023 then profit else 0 end) as profit_2023 
        from cte 
        group by sub_category) cte2 
    order by profit desc 
    limit 1;