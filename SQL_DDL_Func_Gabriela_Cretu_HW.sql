

-- Task 1. Create a view
-- Create a view called 'sales_revenue_by_category_qtr' that displays the film category and 
-- total sales revenue for the current quarter and year. 
-- Only include categories with at least one sale in the current quarter.
-- Note: When the next quarter begins, it will be considered the current quarter.

DROP view sales_revenue_by_category_qtr;

CREATE VIEW sales_revenue_by_category_qtr AS
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
CREATE OR REPLACE FUNCTION get_sales_revenue_by_category_qtr(p_quarter INT, p_year INT)
RETURNS TABLE (
    category_id INT,
    name VARCHAR,
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
    WHERE EXTRACT(QUARTER FROM p.payment_date) = p_quarter  -- Use the passed quarter
      AND EXTRACT(YEAR FROM p.payment_date) = p_year      -- Use the passed year
    GROUP BY c.category_id, c."name"
    HAVING SUM(p.amount) > 0;  -- Only include categories with at least one sale
END;
$$ LANGUAGE plpgsql;

--solution 2
CREATE OR REPLACE FUNCTION get_sales_revenue_by_category_qtr(p_quarter INT, p_year INT, OUT category_id INT, OUT name VARCHAR, OUT total_revenue NUMERIC)
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
    WHERE EXTRACT(QUARTER FROM p.payment_date) = p_quarter
      AND EXTRACT(YEAR FROM p.payment_date) = p_year
    GROUP BY c.category_id, c."name"
    HAVING SUM(p.amount) > 0;
END;
$$ LANGUAGE plpgsql;


-- Task 3. Create procedure language functions
-- Create a function that takes a country as an input parameter and returns the 
-- most popular film in that country. 
-- Example Query: 
-- SELECT * FROM core.most_popular_films_by_countries(array['Afghanistan','Brazil','United States']);

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
    v_film_name TEXT;
BEGIN
    -- Check if input is empty
    IF array_length(countries, 1) IS NULL THEN
        RAISE EXCEPTION 'Input country list is empty. Please provide at least one country.';
    END IF;

    -- Create a temporary table to store the result
    CREATE TEMPORARY TABLE result_table2 (
        country TEXT,
        film_name TEXT,
        rating TEXT,
        language_name BPCHAR,
        film_length INT,
        release_year public."year"
    ) ON COMMIT DROP;

    -- Loop through each country in the input array
    FOREACH country_name IN ARRAY countries LOOP
        RAISE NOTICE 'Now we are searching for the most viewed film in %', country_name;

        -- Check if country exists in the database
        SELECT COUNT(*) INTO v_count
        FROM public.country c2
        WHERE c2.country = country_name;

        IF v_count = 0 THEN
            RAISE WARNING 'Country "%" not found in the database. Skipping.', country_name;
            CONTINUE;
        END IF;

        -- Insert the most rented film for the current country into the result table
        INSERT INTO result_table2 (
            country,
            film_name,
            rating,
            language_name,
            film_length,
            release_year
        )
        SELECT
            country_name,
            f.title,
            f.rating::TEXT,
            l.name,
            f.length,
            f.release_year
        FROM public.country c2
        LEFT JOIN public.city c ON c.country_id = c2.country_id
        LEFT JOIN public.address a ON a.city_id = c.city_id
        LEFT JOIN public.store s ON s.address_id = a.address_id
        LEFT JOIN public.staff sa ON sa.store_id = s.store_id
        LEFT JOIN public.rental r ON r.staff_id = sa.staff_id
        LEFT JOIN public.inventory i ON i.inventory_id = r.inventory_id
        LEFT JOIN public.film f ON f.film_id = i.film_id
        LEFT JOIN public.language l ON f.language_id = l.language_id
        WHERE c2.country = country_name
        GROUP BY f.film_id, f.title, f.rating, l.name, f.length, f.release_year
        ORDER BY COUNT(r.rental_id) DESC
        LIMIT 1;

        -- Check if insert happened
        SELECT r.film_name INTO v_film_name
        FROM result_table2 r
        WHERE r.country = country_name;

        IF v_film_name IS NULL THEN
            RAISE NOTICE 'No rentals found for country "%".', country_name;
        END IF;
    END LOOP;

    -- Return all the records from the temporary table
    RETURN QUERY
    SELECT
        re.country,
        re.film_name,
        re.rating,
        re.language_name,
        re.film_length,
        re.release_year
    FROM result_table2 re;
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

DROP FUNCTION IF EXISTS films_in_stock_by_title(TEXT);

DROP FUNCTION IF EXISTS films_in_stock_by_title(TEXT);

CREATE OR REPLACE FUNCTION films_in_stock_by_title(word TEXT)
RETURNS TABLE (
    Row_num BIGINT,
    Film_title TEXT,
    Language BPCHAR,
    Customer_name TEXT,
    Rental_date TIMESTAMPTZ,
    Message TEXT
) AS
$$
BEGIN
    -- Return movies with a partial title match, including row number
    RETURN QUERY 
    SELECT 
        ROW_NUMBER() OVER () AS Row_num,  -- Generates the Row_num
        f.title AS Film_title,
        l.name AS Language,
        c.first_name || ' ' || c.last_name AS Customer_name,
        r.rental_date AS Rental_date,
        NULL AS Message  -- No message when movies are found
    FROM film f
    INNER JOIN language l ON f.language_id = l.language_id
    INNER JOIN public.inventory i ON f.film_id = i.film_id
    INNER JOIN public.rental r ON i.inventory_id = r.inventory_id
    INNER JOIN public.payment p ON r.rental_id = p.rental_id
    INNER JOIN public.customer c ON p.customer_id = c.customer_id
    WHERE f.title ILIKE '%' || word || '%';

    -- If no movies are found, return a message
     IF NOT FOUND THEN
        RAISE EXCEPTION 'No movies found matching the search criteria for "%".', word;
    END IF;
END;
$$ LANGUAGE plpgsql;




SELECT * FROM films_in_stock_by_title('%love%')




-- Task 5. Create procedure language functions
-- Create a function 'new_movie' that inserts a new movie with the given title into the film table.
-- The function should:
--   - Generate a new unique film ID.
--   - Set rental rate to 4.99, rental duration to 3 days, replacement cost to 19.99.
--   - Default release year to the current year and language to 'Klingon'.
--   - Verify the language exists in the 'language' table.
--   - Ensure that the function is not already created before, and replace it if so.

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



