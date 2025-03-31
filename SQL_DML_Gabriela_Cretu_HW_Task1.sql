--All new & updated records must have 'last_update' field set to current_date.
--Double-check your DELETEs and UPDATEs with SELECT query before committing the transaction!!! 
--Your scripts must be rerunnable/reusable and don't produces duplicates. You can use WHERE NOT EXISTS, IF NOT EXISTS, ON CONFLICT DO NOTHING, etc.
--Don't hardcode IDs. Instead of construction INSERT INTO … VALUES use INSERT INTO … SELECT …
--Don't forget to add RETURNING
--Please add comments why you chose a particular way to solve each tasks.

-- Task 1: Adding top-3 favorite movies to 'film' table
-- Ensuring the script is rerunnable using "ON CONFLICT DO NOTHING"
INSERT INTO public.film (title, description, release_year, language_id, original_language_id, rental_duration, rental_rate, length, replacement_cost, rating, last_update, special_features, fulltext)
VALUES 
('Good Will Hunting', 'A young janitor at MIT discovers his exceptional mathematical abilities but struggles with personal and emotional challenges.', 
 1997, 1, NULL, 7, 3.99, 126, 19.99, 'R', CURRENT_TIMESTAMP, '{"Behind the Scenes", "Deleted Scenes"}', 'good will hunting math genius drama'),
('A Beautiful Mind', 'The true story of John Nash, a brilliant but asocial mathematician, who battles schizophrenia while making groundbreaking contributions to game theory.', 
 2001, 1, NULL, 7, 3.99, 135, 19.99, 'PG-13', CURRENT_TIMESTAMP, '{"Director Commentary", "Making-of Featurette"}', 'beautiful mind game theory mathematics biography'),
('The Imitation Game', 'Alan Turing, a brilliant mathematician and logician, works to crack the German Enigma code during World War II, changing the course of history.', 
 2014, 1, NULL, 7, 4.99, 113, 24.99, 'PG-13', CURRENT_TIMESTAMP, '{"Historical Documentary", "Interviews"}', 'imitation game alan turing codebreaking WWII')
ON CONFLICT (title) DO NOTHING
RETURNING film_id, title;

-- Task 1b: Updating rental rates and durations safely
UPDATE public.film f
SET rental_rate = CASE 
    WHEN f.title = 'Good Will Hunting' THEN 4.99
    WHEN f.title = 'A Beautiful Mind' THEN 9.99
    WHEN f.title = 'The Imitation Game' THEN 19.99
    ELSE f.rental_rate
END,
rental_duration = CASE 
    WHEN f.title = 'Good Will Hunting' THEN 1
    WHEN f.title = 'A Beautiful Mind' THEN 2
    WHEN f.title = 'The Imitation Game' THEN 3
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
ON CONFLICT (first_name, last_name) DO NOTHING
RETURNING actor_id, first_name, last_name;

-- Inserting actor-film relationships safely
INSERT INTO public.film_actor (actor_id, film_id, last_update)
SELECT a.actor_id, f.film_id, CURRENT_TIMESTAMP
FROM public.actor a
JOIN public.film f ON 
    (a.first_name = 'Matt' AND a.last_name = 'Damon' AND f.title = 'Good Will Hunting') OR
    (a.first_name = 'Robin' AND a.last_name = 'Williams' AND f.title = 'Good Will Hunting') OR
    (a.first_name = 'Russell' AND a.last_name = 'Crowe' AND f.title = 'A Beautiful Mind') OR
    (a.first_name = 'Jennifer' AND a.last_name = 'Connelly' AND f.title = 'A Beautiful Mind') OR
    (a.first_name = 'Benedict' AND a.last_name = 'Cumberbatch' AND f.title = 'The Imitation Game') OR
    (a.first_name = 'Keira' AND a.last_name = 'Knightley' AND f.title = 'The Imitation Game')
ON CONFLICT DO NOTHING
RETURNING actor_id, film_id;

-- Task 1d: Adding movies to store inventory
INSERT INTO public.inventory (film_id, store_id, last_update)
SELECT f.film_id, s.store_id, CURRENT_TIMESTAMP
FROM public.film f
CROSS JOIN (VALUES (1), (2)) AS s(store_id)
WHERE f.title IN ('Good Will Hunting', 'A Beautiful Mind', 'The Imitation Game')
ON CONFLICT DO NOTHING
RETURNING inventory_id, film_id, store_id;

-- Task 1e: Updating a customer safely
UPDATE public.customer
SET first_name = 'Gabriela', 
    last_name = 'Cretu', 
    email = 'gaby793.2002@gmail.com',
    address_id = 26,  
    last_update = CURRENT_TIMESTAMP
WHERE customer_id = (SELECT customer_id FROM public.customer c
                     JOIN (SELECT customer_id FROM public.rental GROUP BY customer_id HAVING COUNT(*) >= 43) r
                     ON c.customer_id = r.customer_id
                     JOIN (SELECT customer_id FROM public.payment GROUP BY customer_id HAVING COUNT(*) >= 43) p
                     ON c.customer_id = p.customer_id
                     LIMIT 1)
RETURNING customer_id, first_name, last_name;

-- Task 1f: Deleting rental and payment records safely
DELETE FROM public.rental WHERE customer_id = 1 RETURNING rental_id;
DELETE FROM public.payment WHERE customer_id = 1 RETURNING payment_id;

-- Task 1g: Renting and paying for movies
INSERT INTO public.rental (rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
SELECT '2017-01-18', i.inventory_id, 1, NULL, s.staff_id, CURRENT_TIMESTAMP
FROM public.inventory i
JOIN public.staff s ON i.store_id = s.store_id
WHERE i.film_id IN (SELECT film_id FROM public.film WHERE title IN ('Good Will Hunting', 'A Beautiful Mind', 'The Imitation Game'))
LIMIT 4
RETURNING rental_id, inventory_id;

-- Adding payments
INSERT INTO public.payment_p2017_01 (customer_id, staff_id, rental_id, amount, payment_date)
SELECT 1, s.staff_id, r.rental_id, f.rental_rate, '2017-01-15'
FROM public.rental r
JOIN public.inventory i ON r.inventory_id = i.inventory_id
JOIN public.film f ON i.film_id = f.film_id
JOIN public.staff s ON r.staff_id = s.staff_id
WHERE r.customer_id = 1
RETURNING payment_id, customer_id, amount;
