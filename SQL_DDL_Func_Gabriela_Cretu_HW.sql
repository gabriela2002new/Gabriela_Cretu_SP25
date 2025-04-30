

-- Task 1. Create a view
-- Create a view called 'sales_revenue_by_category_qtr' that displays the film category and 
-- total sales revenue for the current quarter and year. 
-- Only include categories with at least one sale in the current quarter.
-- Note: When the next quarter begins, it will be considered the current quarter.

DROP view if exists  sales_revenue_by_category_qtr;

CREATE VIEW sales_revenue_by_category_qtr AS
SELECT 
    c.category_id,
    c."name",
    SUM(p.amount) AS total_revenue
FROM payment p
INNER JOIN public.rental r ON p.rental_id = r.rental_id
INNER JOIN public.inventory i ON r.inventory_id = i.inventory_id
INNER JOIN public.film f ON i.film_id = f.film_id
INNER JOIN public.film_category fc ON f.film_id = fc.film_id
INNER JOIN public.category c ON fc.category_id = c.category_id
WHERE EXTRACT(QUARTER FROM p.payment_date) = EXTRACT(QUARTER FROM CURRENT_DATE)
  AND EXTRACT(YEAR FROM p.payment_date) = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY c.category_id, c."name"
HAVING SUM(p.amount) > 0;  -- Only include categories with at least one sale

SELECT * FROM sales_revenue_by_category_qtr;

 


-- Task 2. Create a query language function
-- Create a query language function 'get_sales_revenue_by_category_qtr' that accepts 
-- the current quarter and year as parameters, and returns the same result as the 
-- 'sales_revenue_by_category_qtr' view.

--solution 1
drop function if exists get_sales_revenue_by_category_qtr;

CREATE OR REPLACE FUNCTION get_sales_revenue_by_category_qtr(date1 DATE)
RETURNS TABLE (
    category_id INT,
    name text,
    total_revenue NUMERIC
) AS
$$
BEGIN
    RETURN QUERY
    SELECT 
        c.category_id,
        c."name",
        SUM(p.amount) AS total_revenue
    FROM payment p
    INNER JOIN public.rental r ON p.rental_id = r.rental_id
    INNER JOIN public.inventory i ON r.inventory_id = i.inventory_id
    INNER JOIN public.film f ON i.film_id = f.film_id
    INNER JOIN public.film_category fc ON f.film_id = fc.film_id
    INNER JOIN public.category c ON fc.category_id = c.category_id
    WHERE EXTRACT(QUARTER FROM p.payment_date) = extract(quarter from date1)  -- Use the passed quarter
      AND EXTRACT(YEAR FROM p.payment_date) =extract(year from date1)     -- Use the passed year
    GROUP BY c.category_id, c."name"
    HAVING SUM(p.amount) > 0;  -- Only include categories with at least one sale
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_sales_revenue_by_category_qtr('12-02-2017');


--solution 2
drop function if exists get_sales_revenue_by_category_qtr;
CREATE OR REPLACE FUNCTION get_sales_revenue_by_category_qtr(date1 date, OUT category_id INT, OUT name text, OUT total_revenue NUMERIC)
returns setof record
AS
$$
BEGIN
    -- use the OUT parameters to return the results.
    RETURN QUERY
    SELECT 
        c.category_id,
        c."name",
        SUM(p.amount) AS total_revenue
    FROM payment p
    INNER JOIN public.rental r ON p.rental_id = r.rental_id
    INNER JOIN public.inventory i ON r.inventory_id = i.inventory_id
    INNER JOIN public.film f ON i.film_id = f.film_id
    INNER JOIN public.film_category fc ON f.film_id = fc.film_id
    INNER JOIN public.category c ON fc.category_id = c.category_id
    WHERE EXTRACT(QUARTER FROM p.payment_date) = extract(quarter from date1)
      AND EXTRACT(YEAR FROM p.payment_date) = extract(year from date1)
    GROUP BY c.category_id, c."name"
    HAVING SUM(p.amount) > 0;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_sales_revenue_by_category_qtr('12-02-2017');



-- Task 3. Create procedure language functions
-- Create a function that takes a country as an input parameter and returns the 
-- most popular film in that country. 
-- Example Query: 
-- SELECT * FROM core.most_popular_films_by_countries(array['Afghanistan','Brazil','United States']);

drop function if exists get_most_popular_films;

CREATE OR REPLACE FUNCTION get_most_popular_films(countries TEXT[])
RETURNS TABLE (
    country TEXT,
    film_name TEXT,
    rating TEXT,
    language_name BPCHAR,
    film_length INT,
    release_year public."year"
) AS
$$
DECLARE
    country_name TEXT;
    v_count INT;
    v_rental_count INT;
BEGIN
    -- Check if input is empty
    IF array_length(countries, 1) IS NULL THEN
        RAISE EXCEPTION 'Input country list is empty. Please provide at least one country.';
    END IF;

    -- Loop through each country individually
    FOREACH country_name IN ARRAY countries LOOP
        RAISE NOTICE 'Now we are searching for the most viewed film in %', country_name;

        -- Check if country exists
        SELECT COUNT(*) INTO v_count
        FROM public.country c
        WHERE lower(c.country) = lower(country_name);

        IF v_count = 0 THEN
            RAISE WARNING 'Country "%" not found in the database. Skipping.', country_name;
            CONTINUE;
        END IF;

        -- Check if there are rentals for that country
        SELECT COUNT(r.rental_id) INTO v_rental_count
        FROM public.country c2
        JOIN public.city c ON c.country_id = c2.country_id
        JOIN public.address a ON a.city_id = c.city_id
        JOIN public.store s ON s.address_id = a.address_id
        JOIN public.staff sa ON sa.store_id = s.store_id
        JOIN public.rental r ON r.staff_id = sa.staff_id
        WHERE lower(c2.country) = lower(country_name);

        IF v_rental_count = 0 THEN
            RAISE NOTICE 'No rentals found for country "%". Skipping.', country_name;
            CONTINUE;
        END IF;

        -- For valid country with rentals, return the result
        RETURN QUERY
        WITH ranked_films AS (
            SELECT
                country_name AS country,
                f.title AS film_name,
                f.rating::TEXT AS rating,
                l.name AS language_name,
                f.length::INT AS film_length, -- Cast smallint to int here!
                f.release_year,
                DENSE_RANK() OVER (ORDER BY COUNT(r.rental_id) DESC) AS rnk
            FROM public.country c2
            JOIN public.city c ON c.country_id = c2.country_id
            JOIN public.address a ON a.city_id = c.city_id
            JOIN public.store s ON s.address_id = a.address_id
            JOIN public.staff sa ON sa.store_id = s.store_id
            JOIN public.rental r ON r.staff_id = sa.staff_id
            JOIN public.inventory i ON i.inventory_id = r.inventory_id
            JOIN public.film f ON f.film_id = i.film_id
            JOIN public.language l ON f.language_id = l.language_id
            WHERE lower(c2.country) = lower(country_name)
            GROUP BY f.film_id, f.title, f.rating, l.name, f.length, f.release_year
        )
        SELECT
            r.country,
            r.film_name,
            r.rating,
            r.language_name,
            r.film_length,
            r.release_year
        FROM ranked_films r
        WHERE rnk = 1
        ORDER BY film_name;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- Example usage
SELECT * FROM get_most_popular_films(ARRAY['Australia', 'Brazil', 'United States']);


 
-- Task 4. Create procedure language functions
-- Create a function to generate a list of movies available in stock based on a partial 
-- title match (e.g., containing 'love' in the title). If no movie is found, return 
-- a message indicating the absence of the movie in stock.
-- The titles should be formatted as '%...%' and the results should include a row number 
-- (starting from 1 and incrementing). 
-- Example Query: 
-- SELECT * FROM core.films_in_stock_by_title('%love%');


drop function if exists  films_in_stock_by_title;
CREATE OR REPLACE FUNCTION films_in_stock_by_title(
    IN word TEXT,
    OUT r_num BIGINT,
    OUT Film_title TEXT,
    OUT Language TEXT,
    OUT Customer_name TEXT,
    OUT Rental_date TIMESTAMPTZ
)
RETURNS SETOF RECORD
AS
$$
DECLARE
    rec RECORD;
    counter BIGINT := 1;
BEGIN
    FOR rec IN
        SELECT 
            f.title AS Film_title,
            l.name AS Language,
            c.first_name || ' ' || c.last_name AS Customer_name,
            r.rental_date AS Rental_date
        FROM film f
        INNER JOIN language l ON f.language_id = l.language_id
        INNER JOIN inventory i ON f.film_id = i.film_id
        INNER JOIN rental r ON i.inventory_id = r.inventory_id
        INNER JOIN payment p ON r.rental_id = p.rental_id
        INNER JOIN customer c ON p.customer_id = c.customer_id
        WHERE f.title ILIKE '%' || word || '%'
    LOOP
        r_num := counter;
        Film_title := rec.Film_title;
        Language := rec.Language;
        Customer_name := rec.Customer_name;
        Rental_date := rec.Rental_date;

        counter := counter + 1;

        RETURN NEXT; -- return one row at a time
    END LOOP;

    IF counter = 1 THEN -- No rows were returned
        RAISE NOTICE 'No movies in stock matching the search criteria for "%".', word;
    END IF;

    RETURN;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM films_in_stock_by_title('%loved%')



-- Task 5. Create procedure language functions
-- Create a function 'new_movie' that inserts a new movie with the given title into the film table.
-- The function should:
--   - Generate a new unique film ID.
--   - Set rental rate to 4.99, rental duration to 3 days, replacement cost to 19.99.
--   - Default release year to the current year and language to 'Klingon'.
--   - Verify the language exists in the 'language' table.
--   - Ensure that the function is not already created before, and replace it if so.

drop function if exists  new_movie;

CREATE OR REPLACE FUNCTION new_movie(
    movie TEXT,
    rental_rate NUMERIC DEFAULT 4.99,
    rental_duration INT DEFAULT 3,
    replacement_cost NUMERIC DEFAULT 19.99
)
RETURNS INT AS $$
DECLARE
    m_film_id INT;
    m_language_id INT;
BEGIN
    -- Check if the language 'Klingon' exists in the language table
    SELECT language_id INTO m_language_id
    FROM public.language
    WHERE name = 'Klingon';

    -- If the language does not exist, insert it and get the language_id
    IF NOT FOUND THEN
        INSERT INTO public.language(name)
        VALUES ('Klingon')
        RETURNING language_id INTO m_language_id;
    END IF;

    -- Check if the movie already exists in the film table
    SELECT film_id INTO m_film_id
    FROM public.film
    WHERE title = movie;

    IF FOUND THEN
        -- If the movie already exists, raise a notice and return the existing film_id
        RAISE NOTICE 'Movie "%" already exists with ID: %', movie, m_film_id;
        RETURN m_film_id;
    END IF;

    -- Insert the new movie into the film table
    INSERT INTO public.film(title, rental_rate, rental_duration, replacement_cost, release_year, language_id)
    VALUES (movie, rental_rate, rental_duration, replacement_cost, EXTRACT(YEAR FROM CURRENT_DATE), m_language_id)
    RETURNING film_id INTO m_film_id;

    -- Display a notice with the film ID
    RAISE NOTICE 'New movie added! ID=%', m_film_id;

    -- Return the new film ID
    RETURN m_film_id;
END;
$$ LANGUAGE plpgsql;

-- Call the function to add a movie with custom values
SELECT new_movie('Star Trek', 5.99, 7, 25.99);



