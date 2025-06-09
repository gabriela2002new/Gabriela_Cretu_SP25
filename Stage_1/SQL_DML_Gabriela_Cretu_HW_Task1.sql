--All new & updated records must have 'last_update' field set to current_date.
--Double-check your DELETEs and UPDATEs with SELECT query before committing the transaction!!! 
--Your scripts must be rerunnable/reusable and don't produces duplicates. You can use WHERE NOT EXISTS, IF NOT EXISTS, ON CONFLICT DO NOTHING, etc.
--Don't hardcode IDs. Instead of construction INSERT INTO … VALUES use INSERT INTO … SELECT …
--Don't forget to add RETURNING
--Please add comments why you chose a particular way to solve each tasks.

-- Task 1: Adding top-3 favorite movies to 'film' table
-- Ensuring the script is rerunnable using "ON CONFLICT DO NOTHING"
--improvemnet: instead of hardcoding the language I used (SELECT language_id FROM language WHERE name = 'English') to find the position

ALTER TABLE public.film ADD CONSTRAINT unique_film_title UNIQUE (title);


INSERT INTO public.film (title, description, release_year, language_id, original_language_id, rental_duration, rental_rate, length, replacement_cost, rating, last_update, special_features, fulltext)
VALUES 
('Good Will Hunting', 'A young janitor at MIT discovers his exceptional mathematical abilities but struggles with personal and emotional challenges.', 
 1997, (SELECT language_id FROM language WHERE name = 'English'), NULL, 7, 3.99, 126, 19.99, 'R', CURRENT_TIMESTAMP, '{"Behind the Scenes", "Deleted Scenes"}', 'good will hunting math genius drama'),
('A Beautiful Mind', 'The true story of John Nash, a brilliant but asocial mathematician, who battles schizophrenia while making groundbreaking contributions to game theory.', 
 2001, (SELECT language_id FROM language WHERE name = 'English'), NULL, 7, 3.99, 135, 19.99, 'PG-13', CURRENT_TIMESTAMP, '{"Director Commentary", "Making-of Featurette"}', 'beautiful mind game theory mathematics biography'),
('The Imitation Game', 'Alan Turing, a brilliant mathematician and logician, works to crack the German Enigma code during World War II, changing the course of history.', 
 2014, (SELECT language_id FROM language WHERE name = 'English'), NULL, 7, 4.99, 113, 24.99, 'PG-13', CURRENT_TIMESTAMP, '{"Historical Documentary", "Interviews"}', 'imitation game alan turing codebreaking WWII')
ON CONFLICT (title) DO NOTHING
RETURNING film_id, title;


-- Task 1b: Updating rental rates and durations safely
--improvement: I changed the weeks to days 1week->7 days, 2 weeks->14 days, 3 weeks->21 days
UPDATE public.film f
SET rental_rate = CASE 
    WHEN f.title = 'Good Will Hunting' THEN 4.99
    WHEN f.title = 'A Beautiful Mind' THEN 9.99
    WHEN f.title = 'The Imitation Game' THEN 19.99
    ELSE f.rental_rate
END,
rental_duration = CASE 
    WHEN f.title = 'Good Will Hunting' THEN 7
    WHEN f.title = 'A Beautiful Mind' THEN 14
    WHEN f.title = 'The Imitation Game' THEN 21
    ELSE f.rental_duration
END
WHERE f.title IN ('Good Will Hunting', 'A Beautiful Mind', 'The Imitation Game')
RETURNING film_id, title, rental_rate, rental_duration;

-- Task 1c: Adding actors and linking them to films

INSERT INTO public.actor (first_name, last_name, last_update)
VALUES
('Matt', 'Damon', CURRENT_TIMESTAMP),
('Robin', 'Williams', CURRENT_TIMESTAMP),
('Russell', 'Crowe', CURRENT_TIMESTAMP),
('Jennifer', 'Connelly', CURRENT_TIMESTAMP),
('Benedict', 'Cumberbatch', CURRENT_TIMESTAMP),
('Keira', 'Knightley', CURRENT_TIMESTAMP)
ON CONFLICT (actor_id) DO NOTHING
RETURNING actor_id, first_name, last_name;

-- Inserting actor-film relationships safely
INSERT INTO public.film_actor (actor_id, film_id, last_update)
SELECT a.actor_id, f.film_id, CURRENT_TIMESTAMP
FROM public.actor a
INNER JOIN public.film f ON 
    (a.first_name = 'Matt' AND a.last_name = 'Damon' AND f.title = 'Good Will Hunting') OR
    (a.first_name = 'Robin' AND a.last_name = 'Williams' AND f.title = 'Good Will Hunting') OR
    (a.first_name = 'Russell' AND a.last_name = 'Crowe' AND f.title = 'A Beautiful Mind') OR
    (a.first_name = 'Jennifer' AND a.last_name = 'Connelly' AND f.title = 'A Beautiful Mind') OR
    (a.first_name = 'Benedict' AND a.last_name = 'Cumberbatch' AND f.title = 'The Imitation Game') OR
    (a.first_name = 'Keira' AND a.last_name = 'Knightley' AND f.title = 'The Imitation Game')
ON CONFLICT DO NOTHING
RETURNING actor_id, film_id;

-- Task 1d: Adding movies to store inventory with random store assignments
-- improvement:Each movie is assigned to only one random store
WITH RandomStore AS (
    SELECT store_id
    FROM public.store
    ORDER BY random()
    LIMIT 1  -- Select one random store
)
INSERT INTO public.inventory (film_id, store_id, last_update)
SELECT f.film_id, rs.store_id, CURRENT_TIMESTAMP
FROM public.film f
JOIN RandomStore rs ON TRUE  -- Assign the random store to all movies
WHERE f.title IN ('Good Will Hunting', 'A Beautiful Mind', 'The Imitation Game')
ON CONFLICT DO NOTHING
RETURNING inventory_id, film_id, store_id;



-- Task 1e: Updating a customer safely
-- Improvement: avoided hardcoding of IDs, added determinism to selection

UPDATE public.customer
SET first_name = 'Gabriela', 
    last_name = 'Cretu', 
    email = 'gaby793.2002@gmail.com',
    address_id = (
        SELECT address_id 
        FROM address 
        WHERE address = '28 Charlotte Amalie Street' 
        ORDER BY address_id  -- ensure deterministic behavior
        LIMIT 1
    ),  
    last_update = CURRENT_TIMESTAMP
    
WHERE customer_id = (
    SELECT c.customer_id
    FROM public.customer c
    INNER JOIN (
        SELECT customer_id 
        FROM public.rental 
        GROUP BY customer_id 
        HAVING COUNT(*) >= 43
    ) r ON c.customer_id = r.customer_id
    INNER JOIN (
        SELECT customer_id 
        FROM public.payment 
        GROUP BY customer_id 
        HAVING COUNT(*) >= 43
    ) p ON c.customer_id = p.customer_id
    ORDER BY c.customer_id  -- added for determinism
    LIMIT 1
)
RETURNING customer_id, first_name, last_name;


-- Task 1f: Deleting rental and payment records safely
--improvement: excluded the hardcoding

DELETE FROM public.payment 
WHERE customer_id = (
    SELECT customer_id
    FROM public.customer
    WHERE first_name = 'Gabriela'
      AND last_name = 'Cretu'
      AND email = 'gaby793.2002@gmail.com'
) RETURNING payment_id;

DELETE FROM public.rental 
WHERE customer_id = (
    SELECT customer_id
    FROM public.customer
    WHERE first_name = 'Gabriela'
      AND last_name = 'Cretu'
      AND email = 'gaby793.2002@gmail.com'
) RETURNING rental_id;





--Task 1g:Rent you favorite movies from the store they are in and pay for them (add corresponding records to the database to represent 
--this activity)(Note: to insert the payment_date into the table payment, you can create a new partition (see the scripts to install the 
--training database ) or add records for the first half of 2017)

-- Step 0: Define the function to compute the payment
CREATE OR REPLACE FUNCTION calculate_payment_for_rental(
    p_rental_date TIMESTAMPTZ,
    p_return_date TIMESTAMPTZ,
    p_rental_duration SMALLINT,
    p_rental_duration3 INT,
    p_replacement_cost NUMERIC,
    p_rental_rate NUMERIC,
    p_overdue_rate NUMERIC
)
RETURNS NUMERIC AS $$
DECLARE
    v_actual_duration INT;
    v_payment NUMERIC;
BEGIN
    v_actual_duration := EXTRACT(DAY FROM (p_return_date - p_rental_date))::INT;

    IF v_actual_duration > p_rental_duration3 THEN
        v_payment := p_replacement_cost;
    ELSIF v_actual_duration <= p_rental_duration THEN
        v_payment := p_rental_rate;
    ELSIF v_actual_duration > p_rental_duration AND v_actual_duration <= p_rental_duration3 THEN
        v_payment := p_rental_rate + (v_actual_duration - p_rental_duration) * p_overdue_rate;
    ELSE
        v_payment := 0;
    END IF;

    RETURN v_payment;
END;
$$ LANGUAGE plpgsql;


-- Step 1: Define the CTE for the selected films, staff, and inventory+ insert into the rental table
WITH selected_films AS (
    SELECT f.film_id,f.title, MIN(st.staff_id) AS staff_id, i.inventory_id  -- Selects one staff_id per film(I assumed that given all the movies are at the same 
                                                                           --store he'll buy all from the same store from the same staff member, you can also use MAX if needed
    FROM inventory i
    INNER JOIN film f ON i.film_id = f.film_id
    INNER JOIN staff st ON i.store_id = st.store_id
    INNER JOIN store s ON st.store_id = s.store_id
    WHERE f.title IN ('Good Will Hunting', 'A Beautiful Mind', 'The Imitation Game')
    GROUP BY f.film_id, f.title, i.inventory_id 
    ORDER BY  f.film_id DESC
),
selected_customer AS (
    SELECT customer_id
    FROM public.customer
    WHERE first_name = 'Gabriela'
      AND last_name = 'Cretu'
      AND email = 'gaby793.2002@gmail.com'
    LIMIT 1
)
INSERT INTO public.rental (
    rental_date, 
    inventory_id, 
    customer_id, 
    return_date, 
    staff_id, 
    last_update
)
SELECT 
    '2017-01-01',  -- Current date and time as rental_date
    sf.inventory_id,
    sc.customer_id, 
    '2017-01-31',  -- Set return_date end of the month
    sf.staff_id,  -- Use the staff_id for the film
    NOW()  -- last_update is set to current date and time
FROM selected_films sf
CROSS JOIN selected_customer sc
ON CONFLICT  DO NOTHING
returning rental_date, inventory_id, customer_id, return_date, staff_id, last_update ;


-- Step 2: Define the CTE for the selected films, staff, and inventory+ insert into the payment table
WITH selected_films AS (
    SELECT f.film_id,f.title, MIN(st.staff_id) AS staff_id, i.inventory_id  -- Selects one staff_id per film, you can also use MAX if needed
    FROM inventory i
    INNER JOIN film f ON i.film_id = f.film_id
    INNER JOIN staff st ON i.store_id = st.store_id
    INNER JOIN store s ON st.store_id = s.store_id
    WHERE f.title IN ('Good Will Hunting', 'A Beautiful Mind', 'The Imitation Game')
    GROUP BY f.film_id, f.title, i.inventory_id 
    ORDER BY  f.film_id DESC
),
selected_customer AS (
    SELECT customer_id
    FROM public.customer
    WHERE first_name = 'Gabriela'
      AND last_name = 'Cretu'
      AND email = 'gaby793.2002@gmail.com'
    LIMIT 1
)
INSERT INTO public.payment (
    customer_id, 
    staff_id, 
    rental_id, 
    amount, 
    payment_date
)
SELECT 
    sc.customer_id, 
    sf.staff_id,  -- Staff ID from the film's associated staff
    r.rental_id, 
    calculate_payment_for_rental(
        r.rental_date,  -- Rental date
        r.return_date,   -- Return date
        f.rental_duration,  -- Rental duration from the film table
        f.rental_duration * 3,  -- rental_duration3 (3 times the rental_duration)
        f.replacement_cost,  -- Replacement cost from the film table
        f.rental_rate,  -- Rental rate from the film table
         1   -- Overdue rate ( + 1 dollar for each overdue day)
    ),
    '2017-01-31'::DATE  -- Fixed payment date in the first half of 2017
FROM rental r
INNER JOIN selected_films sf ON r.inventory_id = sf.inventory_id
INNER JOIN film f ON sf.film_id = f.film_id  -- Join to get rental_duration from the film table
CROSS JOIN selected_customer sc
WHERE r.customer_id = sc.customer_id
ON CONFLICT  DO NOTHING
RETURNING customer_id, staff_id, rental_id, amount, payment_date;





