--Task 1
--Create a query to produce a sales report highlighting 
--the top customers with the highest sales across different sales channels. 
--This report should list 
--the top 5 customers for each channel. 
--Additionally, calculate a key performance indicator (KPI) called 
--'sales_percentage,' which represents the percentage of a customer's sales relative to the total sales within their respective channel.

--Please format the columns as follows:
--Display the total sales amount with two decimal places
--Display the sales percentage with four decimal places and include the percent sign (%) at the end
--Display the result for each channel in descending order of sales


--here I have 2 solutions 
--Solution 1: I compute the percenatge using all the customers (not just those 5), but i display only 5
WITH ranked_sales AS (
  SELECT ch.channel_id, ch.channel_desc,c.cust_first_name,c.cust_last_name, round(sum(s.amount_sold),2) as total_sales,
    row_number() OVER (PARTITION BY ch.channel_id ORDER BY sum(s.amount_sold) DESC) AS sales_rank
  FROM sales s
  LEFT JOIN customers c ON c.cust_id = s.cust_id 
  LEFT JOIN channels ch ON ch.channel_id = s.channel_id
  group by c.cust_id,c.cust_first_name,c.cust_last_name,ch.channel_desc,ch.channel_id
),
total_sales as
(SELECT channel_desc,cust_first_name,cust_last_name, sales_rank, total_sales, sum(total_sales) over (partition by channel_id) as chan_total
FROM ranked_sales)

SELECT channel_desc,cust_first_name,cust_last_name, total_sales as amount_sold, round(total_sales*100.0/chan_total,4) || '%' as sales_percenatge
FROM total_sales
where sales_rank<=5;

--Solution 2: I compute the percenate using only those 5 customers and display 5

WITH ranked_sales AS (
  SELECT ch.channel_id, ch.channel_desc,c.cust_first_name,c.cust_last_name, round(sum(s.amount_sold),2) as total_sales,
    row_number() OVER (PARTITION BY ch.channel_id ORDER BY sum(s.amount_sold) DESC) AS sales_rank
  FROM sales s
  LEFT JOIN customers c ON c.cust_id = s.cust_id 
  LEFT JOIN channels ch ON ch.channel_id = s.channel_id
  group by c.cust_id,c.cust_first_name,c.cust_last_name,ch.channel_desc,ch.channel_id
),
total_sales as (SELECT channel_desc,cust_first_name,cust_last_name, sales_rank, total_sales, sum(total_sales) over (partition by channel_id) as chan_total
FROM ranked_sales
where sales_rank<=5)
select channel_desc,cust_first_name,cust_last_name, total_sales as amount_sold,round(total_sales*100/chan_total,4) || '%' as sales_percentage
from total_sales ;


--Task 2
--Create a query to retrieve data for a report that displays the total sales for all products in 
--the Photo category in the Asian region for the year 2000.

-- Calculate the overall report total and name it 'YEAR_SUM'
--Display the sales amount with two decimal places
--Display the result in descending order of 'YEAR_SUM'
--For this report, consider exploring the use of the crosstab function. 

--here i considered the fiscal year as the year


---crosstab
CREATE EXTENSION IF NOT EXISTS tablefunc;

SELECT *
FROM crosstab(
  $$
  SELECT 
      p.prod_name,
      'YEAR_SUM'::text AS label,  -- only one column, so label is static
      ROUND(SUM(s.amount_sold), 2) AS YEAR_SUM
  FROM sh.sales s
  JOIN sh.products p ON p.prod_id = s.prod_id
  JOIN sh.customers cust ON cust.cust_id = s.cust_id
  JOIN sh.times t ON t.time_id = s.time_id
  JOIN sh.countries cn ON cn.country_id = cust.country_id
  WHERE 
      cn.country_region = 'Asia'
      AND t.fiscal_year = 2000
      AND p.prod_category = 'Photo'
  GROUP BY p.prod_name
  ORDER BY p.prod_name
  $$
) AS ct(prod_name VARCHAR(50), YEAR_SUM NUMERIC);

---no crosstab
SELECT 
    p.prod_name,
    
    ROUND(SUM(CASE 
                WHEN CAST(SUBSTR(t.fiscal_month_desc, 6, 2) AS INTEGER) IN (1, 2, 3) THEN s.amount_sold 
                ELSE 0 
              END), 2) AS q1,
    ROUND(SUM(CASE 
                WHEN CAST(SUBSTR(t.fiscal_month_desc, 6, 2) AS INTEGER) IN (4, 5, 6) THEN s.amount_sold 
                ELSE 0 
              END), 2) AS q2,
    ROUND(SUM(CASE 
                WHEN CAST(SUBSTR(t.fiscal_month_desc, 6, 2) AS INTEGER) IN (7, 8, 9) THEN s.amount_sold 
                ELSE 0 
              END), 2) AS q3,
    ROUND(SUM(CASE 
                WHEN CAST(SUBSTR(t.fiscal_month_desc, 6, 2) AS INTEGER) IN (10, 11, 12) THEN s.amount_sold 
                ELSE 0 
              END), 2) AS q4, ROUND(SUM(s.amount_sold), 2) AS YEAR_SUM
FROM sh.sales s
JOIN sh.products p ON p.prod_id = s.prod_id
JOIN sh.customers cust ON cust.cust_id = s.cust_id
JOIN sh.times t ON t.time_id = s.time_id
JOIN sh.countries cn ON cn.country_id = cust.country_id
WHERE cn.country_region = 'Asia' 
    AND t.fiscal_year = 2000
    AND p.prod_category = 'Photo'
GROUP BY 
    p.prod_name
ORDER BY YEAR_SUM DESC;


--Task 3
--Create a query to generate a sales report for customers ranked in the 
--top 300 based
 --on total sales in the years 1998, 1999, and 2001.
 -- The report should be categorized based on sales channels, 
 --and separate calculations should be performed for each channel.

--Retrieve customers who ranked among the top 300 in sales for the years 1998, 1999, and 2001
--Categorize the customers based on their sales channels
--Perform separate calculations for each sales channel
--Include in the report only purchases made on the channel specified
--Format the column so that total sales are displayed with two decimal places

--Solution 1:I used the fiscal year to recover the years and then i considered the first 300 for each channel in each year 1999,1998,2001
--a cte is faster
WITH RankedSales AS (
    -- Calculate total sales per customer per channel and rank them
    select 
        c.cust_id,
        c.cust_last_name,
        c.cust_first_name,
        c2.channel_desc,
        c2.channel_id,  -- Add channel_id to SELECT for proper grouping
        t.fiscal_year,
        sum(s.amount_sold) as total_sales,
        rank() over (partition by c2.channel_id order by sum(s.amount_sold) desc) as sales_rank
    from sales s
    left join customers c on c.cust_id = s.cust_id
    left join channels c2 on c2.channel_id = s.channel_id
    left join times t on t.time_id = s.time_id
    where t.fiscal_year in (1998, 1999, 2001)
    group by c.cust_id, c2.channel_desc, c2.channel_id, t.fiscal_year -- Group by channel_id as well
)
-- Select only the top 300 customers for each channel
select 
    
    r.channel_desc,r.cust_id,r.cust_last_name, r.cust_first_name,
    round(r.total_sales, 2) as total_sales -- Format total sales with 2 decimal places
from RankedSales r
where r.sales_rank <= 300
order by r.channel_desc, r.sales_rank, r.cust_id;


--Solution 2:I used the fiscal year to recover the years and then i considered the total sales for each channel in each year 1999,1998,2001 for each 
--customer and later I simply extracted the first 300 out of all
--a cte is faster

WITH CustomerYearChannelSales AS (
    SELECT 
        c.cust_id,
        c.cust_last_name,
        c.cust_first_name,
        c2.channel_desc,
        t.fiscal_year,
        SUM(s.amount_sold) AS total_sales
    FROM sales s
    LEFT JOIN customers c ON c.cust_id = s.cust_id
    LEFT JOIN channels c2 ON c2.channel_id = s.channel_id
    LEFT JOIN times t ON t.time_id = s.time_id
    WHERE t.fiscal_year IN (1998, 1999, 2001)
    GROUP BY c.cust_id, c2.channel_desc, t.fiscal_year
),
RankedSales AS (
    SELECT 
        *,
        RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
    FROM CustomerYearChannelSales
)
SELECT 
    r.channel_desc,r.cust_id,r.cust_last_name, r.cust_first_name,
    ROUND(r.total_sales, 2) AS total_sales
FROM RankedSales r
WHERE sales_rank <= 300
ORDER BY sales_rank, fiscal_year, channel_desc, cust_id;



--Solution 3 : Simply for each channel i computed the sales ACROSS all years and then i ranked them to find the first 300

WITH ChannelCustomerSales AS (
    SELECT 
        c.cust_id,
        c2.channel_id,
        c2.channel_desc,
        SUM(s.amount_sold) AS total_sales
    FROM sales s
    JOIN customers c ON c.cust_id = s.cust_id
    JOIN channels c2 ON c2.channel_id = s.channel_id
    JOIN times t ON t.time_id = s.time_id
    WHERE t.fiscal_year IN (1998, 1999, 2001)
    GROUP BY c.cust_id, c2.channel_id, c2.channel_desc
),
RankedSales AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY channel_id ORDER BY total_sales DESC) AS sales_rank
    FROM ChannelCustomerSales
)
SELECT 
    cust_id,
    channel_desc,
    ROUND(total_sales, 2) AS total_sales
FROM RankedSales
WHERE sales_rank <= 300
ORDER BY  sales_rank, cust_id;

--Task 4
--Create a query to generate a sales report for January 2000, February 2000, and March 2000 
--specifically for the Europe and Americas regions.
--Display the result by months and by product category in alphabetical order.

--I considered aggregate functions as this tme the task was not to display any total over a partition or rank
SELECT 
    t.calendar_month_desc, 
    p.prod_category, 
    SUM(CASE WHEN c.country_region = 'Americas' THEN s.amount_sold ELSE 0 END) AS "Americas SALES",
    SUM(CASE WHEN c.country_region = 'Europe' THEN s.amount_sold ELSE 0 END) AS "Europe SALES"
FROM sales s
LEFT JOIN products p ON p.prod_id = s.prod_id 
LEFT JOIN times t ON t.time_id = s.time_id 
LEFT JOIN customers c2 ON c2.cust_id = s.cust_id 
LEFT JOIN countries c ON c.country_id = c2.country_id 
WHERE t.calendar_month_desc IN ('2000-01', '2000-02', '2000-03')
  AND c.country_region IN ('Europe', 'Americas')
GROUP BY t.calendar_month_desc, p.prod_category
ORDER BY t.calendar_month_desc, p.prod_category;

 

