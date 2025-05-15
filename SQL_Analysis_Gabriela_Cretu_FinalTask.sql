--Task 1. Window Functions
--Create a query to generate a report that identifies for each channel and throughout the entire period, the regions with the 
--highest quantity of products sold (quantity_sold). 

--The resulting report should include the following columns:
--CHANNEL_DESC
--COUNTRY_REGION
--SALES: This column will display the number of products sold (quantity_sold) with two decimal places.
--SALES %: This column will show the percentage of maximum sales in the region (as displayed in the SALES column) compared to the total sales for that channel. The sales percentage should be displayed with two decimal places and include the percent sign (%) at the end.
--Display the result in descending order of SALES


WITH sales_overall AS (
    SELECT 
        c3.channel_desc,
        c.country_region,
        SUM(s.quantity_sold) AS sales,
        RANK() OVER (PARTITION BY c3.channel_desc ORDER BY SUM(s.quantity_sold) DESC) AS rank,
        ROUND(
            SUM(s.quantity_sold) * 100.0 / SUM(SUM(s.quantity_sold)) OVER (PARTITION BY c3.channel_desc),
            2
        ) || '%' AS "SALES %"
    FROM 
        sh.sales s
    JOIN 
        sh.customers c2 ON c2.cust_id = s.cust_id
    JOIN 
        sh.countries c ON c.country_id = c2.country_id
    JOIN 
        sh.channels c3 ON s.channel_id = c3.channel_id
    GROUP BY 
        c3.channel_desc,
        c.country_region
)
SELECT channel_desc, country_region, sales, "SALES %"
FROM sales_overall
WHERE rank = 1
ORDER BY sales DESC;






--Task 2. Window Functions
--Identify the subcategories of products with consistently higher sales from 1998 to 2001 compared to the previous year. 
--Determine the sales for each subcategory from 1998 to 2001.
--Calculate the sales for the previous year for each subcategory.
--Identify subcategories where the sales from 1998 to 2001 are consistently higher than the previous year.
--Generate a dataset with a single column containing the identified prod_subcategory values.


WITH all_sales AS (
    SELECT 
        p.prod_subcategory, 
        t.calendar_year,
        SUM(s.amount_sold) AS sales,
        FIRST_VALUE(SUM(s.amount_sold)) OVER (
            PARTITION BY p.prod_subcategory 
            ORDER BY t.calendar_year 
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS prev_year
    FROM sh.sales s
    LEFT JOIN sh.times t ON t.time_id = s.time_id 
    LEFT JOIN sh.products p ON p.prod_id = s.prod_id 
    WHERE t.calendar_year IN (1997,1998,1999,2000,2001)
    GROUP BY p.prod_subcategory, t.calendar_year
),
upward_only AS (
    SELECT * 
    FROM all_sales 
    WHERE sales > prev_year
)
    SELECT prod_subcategory
    FROM upward_only
    GROUP BY prod_subcategory
    HAVING COUNT(*) = 3;

---issue: I did not find any sales in 1997 so therefore i can not compare the sales from 1997 to 1998, so 
-- as i can not compare i found wrong to include 1998 results in the final results
--also I considered hence that the maximum number of years that a the sales of a product from a subcategory has an upaward trend in 3 years
--hence i included only the subcategoryies where the number of years with an upward trend in sales is 3

--Task 3. Window Frames
--Create a query to generate a sales report for the years 1999 and 2000, focusing on quarters and product categories. 
--In the report you have to  analyze the sales of products from the categories 'Electronics,' 'Hardware,' and 'Software/Other,'
-- across the distribution channels 'Partners' and 'Internet'.

   --The resulting report should include the following columns:
   
   
--CALENDAR_YEAR: The calendar year
--CALENDAR_QUARTER_DESC: The quarter of the year
--PROD_CATEGORY: The product category
--SALES$: The sum of sales (amount_sold) for the product category and quarter with two decimal places
--DIFF_PERCENT: Indicates the percentage by which sales increased or decreased compared to the first quarter of the year. 
--For the first quarter, the column value is 'N/A.' The percentage should be displayed with two decimal places and include the
-- percent sign (%) at the end.
--CUM_SUM$: The cumulative sum of sales by quarters with two decimal places
--The final result should be sorted in ascending order based on two criteria: first by 'calendar_year,' then by 'calendar_quarter_desc';
-- and finally by 'sales' descending

SELECT
  t.calendar_year,
  t.calendar_quarter_number,
  p.prod_category,
  t.calendar_quarter_desc,
  ROUND(SUM(s.amount_sold), 2)  AS sales,
  CASE 
    WHEN t.calendar_quarter_number = 1 THEN NULL
    ELSE ROUND(
      (ROUND(SUM(s.amount_sold), 2) - 
       FIRST_VALUE(ROUND(SUM(s.amount_sold), 2)) OVER (
         PARTITION BY p.prod_category 
         ORDER BY t.calendar_year, t.calendar_quarter_number
       )
      )*100.00/ 
      NULLIF(
        FIRST_VALUE(ROUND(SUM(s.amount_sold), 2)) OVER (
          PARTITION BY p.prod_category 
          ORDER BY t.calendar_year, t.calendar_quarter_number
        ), 0
      ), 2) || '%'
  END AS percentage_change_from_first_quarter,
    round((SUM(SUM(s.amount_sold)) OVER (PARTITION BY t.calendar_quarter_desc)),2) AS cum_sum

  

FROM sh.sales s
inner JOIN sh.times t ON t.time_id = s.time_id 
inner JOIN sh.products p ON p.prod_id = s.prod_id 
inner JOIN sh.channels c ON c.channel_id = s.channel_id 

WHERE 
  t.calendar_year IN (1999, 2000)
  AND p.prod_category IN ('Electronics', 'Hardware', 'Software/Other')
  AND c.channel_desc IN ('Partners', 'Internet')

GROUP BY 
  t.calendar_year,
  t.calendar_quarter_number,
  p.prod_category,
  t.calendar_quarter_desc
  
ORDER BY 
  t.calendar_year,
  t.calendar_quarter_number;
