-- Task 2. Implement role-based authentication model for dvd_rental database


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 1: Create a new user with the username "rentaluser" and the password "rentalpassword". Give the user the ability to connect 
--to the database but no other permissions.
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- Create the user with a password
CREATE USER rentaluser WITH PASSWORD 'rentalpassword';

-- Grant the user permission to connect to the database (dvdrental)
GRANT CONNECT ON DATABASE dvdrental TO rentaluser;

-- Revoke all privileges on the public schema (optional and usually not needed if they have none yet)
-- Instead, if you're trying to prevent access, you usually REVOKE USAGE too:
REVOKE ALL ON SCHEMA public FROM rentaluser;
REVOKE USAGE ON SCHEMA public FROM rentaluser;  -- Optional but safer


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 2: Grant "rentaluser" SELECT permission for the "customer" table. Сheck to make sure this permission works correctly—write a SQL
-- query to select all customers.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

GRANT USAGE ON SCHEMA public TO rentaluser;
GRANT SELECT ON TABLE customer TO rentaluser;
SELECT * FROM customer;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 3: Create a new user group called "rental" and add "rentaluser" to the group
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

 CREATE ROLE rental;
 GRANT rental TO rentaluser;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 4: Grant the "rental" group INSERT and UPDATE permissions for the "rental" table. Insert a new row and update one existing row in the "rental"
-- table under that role. 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
GRANT INSERT on rental TO rentaluser;
GRANT  UPDATE ON rental TO rentaluser;
GRANT USAGE ON SEQUENCE rental_rental_id_seq to rentaluser;
 --check the foreign keys in order to be able to add new entry in rental
   select *
   from inventory i ;
   select *
   from customer c ;
   select *
   from staff s;
   SET ROLE rentaluser;

  INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id)
  VALUES ('2022-01-01 12:00:00', 40, 24, '2022-01-03 12:00:00', 5);

 --find a primary key to know where you will update
 select*
 from rental;

UPDATE rental SET return_date = CURRENT_TIMESTAMP WHERE rental_id = 6;
    set role postgres;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 5: Revoke the "rental" group's INSERT permission for the "rental" table. Try to insert new rows into the "rental" table make sure this action is denied.
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------


REVOKE INSERT ON rental FROM rental;
  SET ROLE rental;

INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id)
  VALUES ('2025-01-01 12:00:00', 40, 24, '2025-01-03 12:00:00', 5);

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 6: Create a personalized role for any customer already existing in the dvd_rental database. The name of the role name must be 
--client_{first_name}_{last_name} (omit curly brackets). The customer's payment and rental history must not be empty. 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 CREATE ROLE clients NOLOGIN;

CREATE TABLE role_customer_map (
    role_name TEXT PRIMARY KEY,
    customer_id INTEGER
);


DO $$
DECLARE
    r RECORD;
    role_name TEXT;
BEGIN
    -- Loop through customers with rental and payment history
    FOR r IN 
        SELECT c.first_name, c.last_name, c.customer_id
        FROM customer c
        WHERE EXISTS (SELECT 1 FROM rental WHERE customer_id = c.customer_id)
          AND EXISTS (SELECT 1 FROM payment WHERE customer_id = c.customer_id)
    LOOP
        -- Create the role name for the client
        role_name := lower('client_' || r.first_name || '_' || r.last_name);

        -- Create the role (if it doesn't already exist)
        EXECUTE format('CREATE ROLE %I LOGIN;', role_name);

        -- Grant the 'clients' group role to the new client role
        EXECUTE format('GRANT clients TO %I;', role_name);

        -- Insert mapping for future reference (if using role_customer_map)
        INSERT INTO role_customer_map(role_name, customer_id)
        VALUES (role_name, r.customer_id)
        ON CONFLICT DO NOTHING;
    END LOOP;
END $$;




SELECT rolname
FROM pg_roles
WHERE rolname LIKE 'client_%';






-- Task 3. Implement row-level security
-- Read about row-level security (https://www.postgresql.org/docs/12/ddl-rowsecurity.html) 
--Configure that role so that the customer can only access their own data in the "rental" and "payment" tables. 
--Write a query to make sure this user sees only their own data.





CREATE POLICY customer_access_policy_payment ON payment
FOR SELECT
USING (
    EXISTS (
        SELECT 1
        FROM role_customer_map rcm
        WHERE rcm.role_name = current_user
          AND rcm.customer_id = payment.customer_id
    )
);

CREATE POLICY customer_access_policy_rentals ON rental
FOR SELECT
USING (
    EXISTS (
        SELECT 1
        FROM role_customer_map rcm
        WHERE rcm.role_name = current_user
          AND rcm.customer_id = rental.customer_id
    )
);

ALTER TABLE payment ENABLE ROW LEVEL SECURITY;
ALTER TABLE rental ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON payment TO clients;
GRANT SELECT ON rental TO clients;

-- Only accessible by clients
REVOKE ALL ON role_customer_map FROM PUBLIC;
GRANT SELECT ON role_customer_map TO clients;

set role client_aaron_selby;
select *
from payment p ;

set role client_aaron_selby;
select *
from rental r;