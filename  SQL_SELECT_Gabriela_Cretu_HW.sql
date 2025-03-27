--This part will check your ability to solve a task with the SQL queries written in different ways, as well as your ability to clarify questions with the reviewer. You are expected to provide at least one solutions for each question below.
 --      1. Different solutions are more about technical part of a solution (e.g. joins, CTEs, subqueries: for example, in the
 -- first solution you solved task through subqueries, try rewriting your query using a CTE). The second option of your SQL query 
 -- should also align with the requirements of the business task.
 --      2. While performing your practice, you can find the missing details in the task description needed to complete it successfully.
 -- Feel free to approach mentors for assistance in our chat.
 --      3. Before each of your query, it would be greatly if you could include the task's conditions as a comment. Additionally,
 -- please provide an explanation of how you interpreted the task's business logic, following our discussion in chat. 
 --      4. Please avoid using AI for generating queries.


--Part 1: Write SQL queries to retrieve the following data
--A.All animation movies released between 2017 and 2019 with rate more than 1, alphabetical


SELECT  f.title -- Included all the titles of movies, ID, rating, and name of category
FROM public.film f
INNER JOIN film_category fc ON f.film_id = fc.film_id
INNER JOIN category c ON fc.category_id = c.category_id  -- Denormalization of the many-to-many relationship between film and category using the bridge table film_category
WHERE f.rental_rate >1  -- a rental rate larger than 1
  AND f.release_year BETWEEN 2017 AND 2019  -- Check if the year is between 2017 and 2019
  AND UPPER(c."name") IN ('ANIMATION')
order by f.title ;  -- Check that the category of the film is 'Animation'

--extension(here I considered a way to find all ratings in order to decide on the rating 1 which I found to be 'G'(general))  
SELECT f.rating, COUNT(*)  -- Select the rating and count the number of films for each rating
FROM public.film f 
WHERE f.release_year BETWEEN 2017 AND 2019  -- Filter films released between 2017 and 2019
GROUP BY f.rating;  -- Group results by rating

--B.The revenue earned by each rental store after March 2017 (columns: address and address2 – as one column, revenue)

SELECT CONCAT(a.address, 
         CASE WHEN a.address2 IS NOT NULL THEN CONCAT(', ', a.address2) ELSE '' end
  ) AS full_address,
       COALESCE(SUM(p.amount), 0) AS revenue -- I used COALESCE to treat NULL values as 0 instead of ignoring them
FROM public.store s--conatenated the two columns in one called full_address
left join address a on s.address_id =a.address_id 
LEFT JOIN inventory i ON s.store_id = i.store_id
LEFT JOIN rental r ON i.inventory_id = r.inventory_id 
LEFT JOIN payment p ON r.rental_id = p.rental_id -- Denormalization of the relationships, starting from store, then inventory, rental, and finally payment
WHERE p.payment_date > '2017-03-31'  -- Filter for payments after March 31, 2017
GROUP BY s.store_id, a.address,a.address2 -- Group by store_id to retrieve the revenue for each store
ORDER BY revenue; -- Order by revenue from lowest to highest

-- C. Top 5 actors by number of movies (released after 2015) they took part in 
-- (columns: first_name, last_name, number_of_movies, sorted by number_of_movies in descending order)

-- Solution 1: (Here I considered actor_id as the main grouping element)
SELECT a.first_name, a.last_name,     COUNT(CASE WHEN f.release_year > 2015 THEN f.film_id END) AS number_of_movies
FROM public.actor a
INNER JOIN film_actor fa ON a.actor_id = fa.actor_id 
inner JOIN film f ON fa.film_id = f.film_id -- Denormalization of the many-to-many relationship between film and actor
GROUP BY a.actor_id, a.first_name, a.last_name -- Grouped by actor_id to ensure uniqueness of actors
ORDER BY number_of_movies DESC -- Ordered from the actor with the most movies to the actor with the least
LIMIT 5; -- Retrieved the top 5 actors


-- Solution 2: (Here I grouped only by a.first_name and a.last_name)
SELECT a.first_name, a.last_name, COUNT(CASE WHEN f.release_year > 2015 THEN f.film_id END) AS number_of_movies
FROM public.actor a
inner JOIN film_actor fa ON a.actor_id = fa.actor_id 
inner JOIN film f ON fa.film_id = f.film_id -- Denormalization of the many-to-many relationship between film and actor
GROUP BY a.first_name, a.last_name -- Grouped by first_name and last_name, potentially combining actors with the same name
ORDER BY number_of_movies DESC -- Ordered from the actor with the most movies to the actor with the least
LIMIT 5; -- Retrieved the top 5 actors

-- Conclusion: I obtained different results because at least  one actor(e.g. Susan Davis) exists under two different actor_ids, 
-- which was not considered in the video. This could indicate either an issue with the database (duplicate records) 
-- or that there are two distinct actors with the same name.

--D.Number of Drama, Travel, Documentary per year (columns: release_year, number_of_drama_movies, number_of_travel_movies, number_of_documentary_movies), sorted by release year in descending order. Dealing with NULL values is encouraged)

-- Solution 1 (Preferred: Here, I followed the instructions, creating separate columns for each film category. 
-- This is more efficient as it results in a total of 31 * 4 cells)

SELECT f.release_year,
       COUNT(CASE WHEN c.name = 'Drama' THEN 1 ELSE NULL END) AS number_of_drama_movies,
       COUNT(CASE WHEN c.name = 'Travel' THEN 1 ELSE NULL END) AS number_of_travel_movies,
       COUNT(CASE WHEN c.name = 'Documentary' THEN 1 ELSE NULL END) AS number_of_documentary_movies
FROM public.film f
LEFT JOIN film_category fc ON f.film_id = fc.film_id
LEFT JOIN category c ON fc.category_id = c.category_id
WHERE UPPER(c.name) IN ('DRAMA', 'TRAVEL', 'DOCUMENTARY')
GROUP BY f.release_year
ORDER BY f.release_year DESC;

-- Solution 2 (Alternative: In this attempt, I grouped by both category and release year. This results in the same outcome as Solution 1, 
-- but is less efficient because it generates 31 * 3 lines, with 3 columns for category name, year, and count—totaling 31 * 3 * 3 cells)
SELECT c."name", f.release_year, COUNT(c.category_id) AS movies
FROM public.film f
LEFT OUTER JOIN film_category fc ON f.film_id = fc.film_id
LEFT OUTER JOIN category c ON fc.category_id = c.category_id -- Denormalization of the many-to-many relationship between film and category through the bridge table film_category
GROUP BY c."name", f.release_year -- Group by both category name and release year to count the number of movies for each category each year
HAVING UPPER(c."name") IN ('DRAMA', 'TRAVEL', 'DOCUMENTARY') -- Only consider movies in these categories
ORDER BY  f.release_year DESC, c."name" DESC; -- Order by category name and release year


--Part 2: Solve the following problems using SQL


--A.Which three employees generated the most revenue in 2017? They should be awarded a bonus for their outstanding performance. 

--Assumptions: 
--staff could work in several stores in a year, please indicate which store the staff worked in (the last one);
--if staff processed the payment then he works in the same store; 
--take into account only payment_date


--First attempt(here I simply considered they worked at the same store during the year)
select s.first_name ,s.last_name ,st.store_id, sum(p.amount)--included the first name last name store id and the total revenue made by each employee
from public.staff s
left join payment p on p.staff_id =s.staff_id 
left join store st on s.store_id=st.store_id -- denormalization of the relationships between staff, payment, store
WHERE extract(year from p.payment_date) = 2017--i considered the payment only during 2017
group by s.first_name ,s.last_name, st.store_id ;-- grouped by the names as well as the store id


--Solution(here I considered the possibility that they may have worked at different stores during a year and considered only the last one)
SELECT s.first_name,s.last_name, st.store_id,SUM(p.amount) AS total_revenue  -- Selecting the first name, last name, store ID , sum up the generated revenue for each staff memberof the staff member
FROM public.staff s  -- Starting from the 'staff' table
inner JOIN payment p ON p.staff_id = s.staff_id  -- Joining the 'payment' table on staff ID to get payments made by the staff
inner JOIN store st ON st.store_id = (  
        SELECT st2.store_id  -- Subquery to find the store where the staff worked during the last payment in 2017
        FROM store st2  -- Querying the 'store' table to get the store ID
        JOIN payment p2 ON p2.staff_id = s.staff_id  -- Joining the 'payment' table to get the payment details
        WHERE p2.payment_date = (  -- Filtering by the most recent payment made by the staff in 2017
            SELECT MAX(p3.payment_date)  -- Finding the latest payment date for that staff member
            FROM payment p3  -- Querying the 'payment' table
            WHERE p3.staff_id = s.staff_id  -- Ensuring the payments are for the current staff member
              AND EXTRACT(YEAR FROM p3.payment_date) = 2017  -- Limiting to payments made in 2017
        )
        LIMIT 1  -- Ensuring we get only one store ID (the store of the last payment in 2017)
    )-- Joining the 'store' table to get the store information for year 2017 for the last store they worked for
WHERE 
    EXTRACT(YEAR FROM p.payment_date) = 2017  -- Filtering to only include payments made in the year 2017
GROUP BY 
    s.first_name, s.last_name, st.store_id  -- Grouping the results by staff name and store ID
ORDER BY 
    total_revenue DESC  -- Ordering by total revenue in descending order (highest revenue first)
LIMIT 3;  -- Limiting the result to the top 3 staff members with the highest total revenue


--Solution:Hanna Carry(79736.45), Hanna Rainbow(40537.94), Peter Lockyard(40077.97)

--B.Which 5 movies were rented more than others (number of rentals), and what's the expected age of the audience for these movies? 
--To determine expected age please use 'Motion Picture Association film rating system


select f.film_id, f.title, f.rating, count(r.rental_id) as rentals, 
    -- Columns selected include the number of rentals for a specific film, film_id, title, rating, and the expected age based on rating
    CASE 
        WHEN f.rating = 'G' THEN 'All Ages'
        WHEN f.rating = 'PG' THEN 'Recommended for 7+'
        WHEN f.rating = 'PG-13' THEN 'Recommended for 13+'
        WHEN f.rating = 'R' THEN 'Restricted, 17+ Only'
        WHEN f.rating = 'NC-17' THEN 'Adults Only, 18+'
        ELSE 'Unknown Rating'
    END AS age_restriction
    -- This CASE statement categorizes the films based on their rating into expected age groups
from public.film f 
inner join inventory i on f.film_id = i.film_id 
inner join rental r on i.inventory_id = r.inventory_id 
    -- Denormalization of the relationship between film_id, inventory, and rentals to allow counting of rentals per film
group by f.film_id, f.title, f.rating 
    -- Grouped by film_id, title, and rating as these columns together define each unique movie
order by rentals desc 
    -- Ordered by number of rentals in descending order
limit 5; 
    -- Limited to the top 5 films based on rentals

--Solution:Using Motion Pictuure association film rating i found that there is a total 5 movies that were rented the most with different age restrictions
--BUCKET BROTHERHOOD: reccomended to 7+
--ROCKETEER MOTHER: reccomended to 13+
--GRIT CLOCKWORK: reccommended to 7+
--RIDGEMONT SUBMARINE: reccomended to 13+
--FORWARD TEMPLE:adults only, over 18

--Part 3. Which actors/actresses didn't act for a longer period of time than the others? 
--The task can be interpreted in various ways, and here are a few options:

-- V1: Gap between the latest release_year and the current year for each actor
SELECT a.actor_id, a.first_name, a.last_name, EXTRACT(YEAR FROM CURRENT_DATE) - f.release_year AS last_gap
    -- Selected actor information (ID, first name, last name), movie release year, and a new column 'last_gap' representing the time between the latest movie and the current year
FROM public.actor a
inner JOIN film_actor fa ON a.actor_id = fa.actor_id
inner JOIN film f ON fa.film_id = f.film_id
    -- Denormalization of the many-to-many relationship between actor and film through the `film_actor` bridge table
WHERE f.release_year = (
    SELECT MAX(f2.release_year)
    FROM film f2
    JOIN film_actor fa2 ON f2.film_id = fa2.film_id
    WHERE fa2.actor_id = a.actor_id
)
    -- The subquery retrieves the most recent movie for each actor, which is then used to calculate the time gap
GROUP BY a.actor_id, a.first_name, a.last_name, f.release_year
    -- Grouped by actor_id, first name, last name, and release year to uniquely identify each actor and their most recent film
ORDER BY last_gap DESC;
    -- Ordered by 'last_gap' in descending order to see the largest gaps first

--Solution: The first one would be Humphrey Garland with a year gap from his last movie in 2015 followed by Penelope Monroe, Russell Bacall,
--Cuba Birch with a 9 year gap from 2016

--V2: gaps between sequential films per each actor;


--Solution 1:(to address efficiency concerns, I used two CTEs: one to capture the actor and release year, and another to calculate the lag years indicating when 
--the actor's previous movie was released.)
WITH FilmOrder AS (
    SELECT 
        fa.actor_id, 
        f.release_year
    FROM public.film_actor fa
    JOIN public.film f ON fa.film_id = f.film_id
), -- This CTE extracts the actor_id and release_year from the film and film_actor tables. It prepares the data to make it easier and faster to calculate the release year gaps in the next CTE.
FilmGaps AS (
    SELECT 
        f1.actor_id,
        f1.release_year AS current_year,
        MAX(f2.release_year) AS previous_year
    FROM FilmOrder f1
    LEFT JOIN FilmOrder f2 
        ON f1.actor_id = f2.actor_id 
        AND f2.release_year < f1.release_year
    GROUP BY f1.actor_id, f1.release_year
) -- This CTE finds, for each actor and each movie, the release year of their most recent previous movie (if any).
SELECT 
    a.actor_id, 
    a.first_name, 
    a.last_name,
    MAX(fg.current_year - fg.previous_year) AS max_gap
FROM public.actor a
JOIN FilmGaps fg ON a.actor_id = fg.actor_id -- Joining actor with FilmGaps to calculate the longest gap for each actor.
WHERE previous_year IS NOT NULL -- Exclude cases where there is no previous movie (i.e., the actor's first movie).
GROUP BY a.actor_id, a.first_name, a.last_name -- Group by actor to calculate the gap per actor.
ORDER BY max_gap DESC, a.last_name, a.first_name; -- Order by the largest gap first, then by last name and first name.

--Solution 2:(less efficient subquery, usig a nested subquery.)
SELECT a.actor_id, a.first_name, a.last_name,
   -- Selected actor information (actor ID, first name, last name) from the `actor` table
   MAX(f.release_year - (
       SELECT MAX(f_prev.release_year)
       FROM public.film_actor fa_prev
       JOIN public.film f_prev ON fa_prev.film_id = f_prev.film_id
       WHERE fa_prev.actor_id = a.actor_id
       AND f_prev.release_year < f.release_year
   )) AS max_gap
   -- This subquery computes the most recent movie released before the current one and calculates the difference in years as `max_gap`
FROM public.actor a
inner JOIN public.film_actor fa ON a.actor_id = fa.actor_id
inner JOIN public.film f ON fa.film_id = f.film_id
   -- Denormalization of the many-to-many relationship between actor and film through the `film_actor` bridge table
GROUP BY a.actor_id, a.first_name, a.last_name
   -- Grouped by actor ID, first name, and last name to uniquely identify each actor
ORDER BY max_gap DESC, a.last_name, a.first_name; -- Ordered by max_gap in descending order to display actors who had the longest gap between films

--Solution:Here we have that largest gap between two consecutive movies of an actor was of 9 year for Minnie Kilmer and Jayne Neeson then 
--followed closely by Laura Brody, Adam Grant, Gary Penn and Burt Posey with a 8 year gap.