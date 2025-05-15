--Task 1
--Create a query for analyzing the annual sales data for the years 1999 to 2001, focusing on different sales channels and regions: 
--'Americas,' 'Asia,' and 'Europe.' 

--The resulting report should contain the following columns:

--The final result should be sorted in ascending order based on three criteria: first by 'country_region,' then by 'calendar_year,' 
--and finally by 'channel_desc'

--AMOUNT_SOLD: This column should show the total sales amount for each sales channel
select s.channel_id, c2.channel_desc,co.country_region, t.calendar_year, sum(amount_sold) as total_sales
from sales s
inner join channels c2 on c2.channel_id=s.channel_id 
inner join customers c on c.cust_id =s.cust_id 
inner join countries co on c.country_id =co.country_id 
inner join times t on t.time_id =s.time_id 
where extract (year from s.time_id) in (1999,2000,2001) and co.country_region in ('Americas', 'Asia', 'Europe')
group by s.channel_id, c2.channel_desc,co.country_region, t.calendar_year 
order by t.calendar_year,country_region, channel_desc;

--% BY CHANNELS: In this column, we should display the percentage of total sales for each channel (e.g. 100% - total 
--sales for Americas in 1999, 63.64% - percentage of sales for the channel “Direct Sales”)

with base_data as
(select s.channel_id, c2.channel_desc,co.country_region, t.calendar_year, sum(amount_sold) as total_sales
from sales s
inner join channels c2 on c2.channel_id=s.channel_id 
inner join customers c on c.cust_id =s.cust_id 
inner join countries co on c.country_id =co.country_id 
inner join times t on t.time_id =s.time_id 
where extract (year from s.time_id) in (1999,2000,2001) and co.country_region in ('Americas', 'Asia', 'Europe')
group by s.channel_id, c2.channel_desc,co.country_region, t.calendar_year 
order by t.calendar_year,country_region, channel_desc)

select channel_id, channel_desc,country_region, calendar_year,total_sales,  round(total_sales/(sum(total_sales) over (partition by (country_region,calendar_year) ))*100, 2) as percentage
from base_data bs
where calendar_year in (1999,2000,2001) and country_region in ('Americas', 'Asia', 'Europe')
order by calendar_year,country_region, channel_desc;

--% PREVIOUS PERIOD: This column should display the same percentage values as in the '% BY CHANNELS' column but for the previous year

with base_data as
(select s.channel_id, c2.channel_desc,co.country_region, t.calendar_year, sum(amount_sold) as total_sales
from sales s
inner join channels c2 on c2.channel_id=s.channel_id 
inner join customers c on c.cust_id =s.cust_id 
inner join countries co on c.country_id =co.country_id 
inner join times t on t.time_id =s.time_id 
where extract (year from s.time_id) in (1998,1999,2000,2001) and co.country_region in ('Americas', 'Asia', 'Europe')
group by s.channel_id, c2.channel_desc,co.country_region, t.calendar_year 
order by t.calendar_year,country_region, channel_desc),

percentage_agg as(select channel_id, channel_desc,country_region, calendar_year,total_sales,  round(total_sales/(sum(total_sales) over (partition by (country_region,calendar_year) ))*100, 2) as percentage
from base_data bs
where calendar_year in (1998,1999,2000,2001) and country_region in ('Americas', 'Asia', 'Europe')
order by calendar_year,country_region, channel_desc)

select channel_id, channel_desc,country_region, calendar_year,total_sales,percentage, FIRST_VALUE(percentage) OVER (
    PARTITION BY (country_region,channel_desc)
    ORDER BY calendar_year
    ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
  ) AS prev_amount
from percentage_agg
order by calendar_year,country_region, channel_desc
offset 12;

--% DIFF: This column should show the difference between the '% BY CHANNELS' and '% PREVIOUS PERIOD' columns, indicating the change in sales percentage from the previous year.

with base_data as
(select s.channel_id, c2.channel_desc,co.country_region, t.calendar_year, sum(amount_sold) as total_sales
from sales s
inner join channels c2 on c2.channel_id=s.channel_id 
inner join customers c on c.cust_id =s.cust_id 
inner join countries co on c.country_id =co.country_id 
inner join times t on t.time_id =s.time_id 
where extract (year from s.time_id) in (1998,1999,2000,2001) and co.country_region in ('Americas', 'Asia', 'Europe')
group by s.channel_id, c2.channel_desc,co.country_region, t.calendar_year 
order by t.calendar_year,country_region, channel_desc),

percentage_agg as(select channel_id, channel_desc,country_region, calendar_year,total_sales,  round(total_sales/(sum(total_sales) over (partition by (country_region,calendar_year) ))*100, 2) as percentage
from base_data bs
where calendar_year in (1998,1999,2000,2001) and country_region in ('Americas', 'Asia', 'Europe')
order by calendar_year,country_region, channel_desc),

prev_table as(select channel_id, channel_desc,country_region, calendar_year,total_sales,percentage, FIRST_VALUE(percentage) OVER (
    PARTITION BY (country_region,channel_desc)
    ORDER BY calendar_year
    ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
  ) AS prev_amount
from percentage_agg
order by calendar_year,country_region, channel_desc
offset 12)

select  country_region,calendar_year, channel_desc,total_sales,percentage,prev_amount, (percentage-prev_amount) as diff
from prev_table
order by country_region, calendar_year, channel_desc;


--Task 2
--You need to create a query that meets the following requirements:
--Generate a sales report for the 49th, 50th, and 51st weeks of 1999.
--Include a column named CUM_SUM to display the amounts accumulated during each week.
--Include a column named CENTERED_3_DAY_AVG to show the average sales for the previous, current, and following days using a centered moving average.
--For Monday, calculate the average sales based on the weekend sales (Saturday and Sunday) as well as Monday and Tuesday.
--For Friday, calculate the average sales on Thursday, Friday, and the weekend.


--Ensure that your calculations are accurate for the beginning of week 49 and the end of week 51.

SELECT 
    t.calendar_week_number, 
    t.time_id, 
    t.day_name,
    SUM(s.amount_sold) AS sales,

    -- CUMULATIVE SUM over the week
    SUM(SUM(s.amount_sold)) OVER (
        PARTITION BY t.calendar_week_number 
        ORDER BY t.time_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_sum,

    -- CENTERED 3-DAY AVERAGE with special logic
    CASE 
        WHEN t.day_name = 'Monday' THEN 
            AVG(SUM(s.amount_sold)) OVER (
                ORDER BY t.time_id 
                RANGE BETWEEN INTERVAL '2 day' PRECEDING AND INTERVAL '1 day' FOLLOWING
            )
        WHEN t.day_name = 'Friday' THEN 
            AVG(SUM(s.amount_sold)) OVER (
                ORDER BY t.time_id 
                RANGE BETWEEN INTERVAL '1 day' PRECEDING AND INTERVAL '2 day' FOLLOWING
            )
        ELSE 
            AVG(SUM(s.amount_sold)) OVER (
                ORDER BY t.time_id 
                RANGE BETWEEN INTERVAL '1 day' PRECEDING AND INTERVAL '1 day' FOLLOWING
            )
    END AS centered_3_day_avg

FROM sales s
JOIN times t ON t.time_id = s.time_id
WHERE t.calendar_year = 1999 
  AND t.calendar_week_number IN (48,49, 50, 51)
GROUP BY t.calendar_week_number, t.time_id, t.day_name
ORDER BY t.calendar_week_number, t.time_id
offset 7;


--Task 3
--Please provide 3 instances of utilizing window functions that include a frame clause, using RANGE, ROWS, and GROUPS modes. 
--Additionally, explain the reason for choosing a specific frame type for each example. 
--This can be presented as a single query or as three distinct queries.


--1. ROWS Mode — Precise Row-Based Frame
--Use Case: Moving average over the current row and its immediate neighbors

SELECT 
    time_id,sum(amount_sold) as sales,
    AVG(sum(amount_sold)) OVER (
        ORDER BY time_id 
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS centered_3_day_avg,
    array_agg(sum(amount_sold)) OVER (
        ORDER BY time_id 
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS values_included
FROM sales
group by time_id;

-- Why?Rows mode is ideal because it uses a fixed sliding window, which is useful when calculating a moving average over time. 
--This approach is effective regardless of the specific time intervals, as it focuses on consecutive logs based on their timestamp.


--2.Range: compares values not row positions
--calculation of a n day rolling sum of sales
SELECT 
    time_id,
    sum(amount_sold) as sales,
    SUM(sum(amount_sold)) OVER (
        ORDER BY time_id 
        RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
    ) AS rolling_7_day_sum
FROM sales
WHERE time_id BETWEEN '1999-12-01' AND '1999-12-31'
group by time_id;

--Why? Because if there are gaps in the data or too many entries for a specific date, we can focus only on a specific time interval.
--Instead of limiting ourselves to a random number of rows, which doesn't guarantee that we're working within the correct timeframe, 
--this approach ensures we stay within the desired window.
--The only drawback is that the sliding window is no longer fixed.

--3.Group:bases  evrything on same values (e.g. if we have 8 9 9 10 10 12 12 12 then when 10 is the current row will consider all rows before with value 9 and also all rows with value 12 and the entire group with values 10 if we we groups 1 preceding 1 following)
--rank group size for products with same price, next following price and the next one

SELECT 
    p.prod_id,
    p.prod_list_price,
    DENSE_RANK() OVER (ORDER BY prod_list_price) AS price_rank,
    COUNT(*) OVER (
        ORDER BY prod_list_price 
        GROUPS BETWEEN CURRENT ROW AND 2 FOLLOWING
    ) AS rank_group_size
FROM products p
order by prod_list_price;

--Why? When you have different groups or essentially the same values across various observations, you may want to know the count 
--of items in each group. However, you might also be interested in 'close groups'—groups with similar values that could have higher 
--counts.
--In such cases, simply using rows won't work because applying a count on that partition may not give you the expected results if 
--you don't know the exact number of elements in each group. This is where the 'group' option comes into play.
--The only downside is that, unlike a window with a fixed number of rows, this approach doesn't have a set number of rows.