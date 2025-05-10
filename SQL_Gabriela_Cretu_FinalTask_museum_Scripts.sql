-- Database & Schema Creation
-- ============================
--first create a postgres connection
drop database if exists museum;
CREATE database if not exists  museum;

--after creation switch connection to this db from new database Connection
--before running this part create a new sql script in the database museum
DROP schema IF EXISTS  museum;
CREATE SCHEMA IF NOT EXISTS museum;

-- 3. Create a physical database with a separate database and schema, assigning an appropriate domain-related name.
-- Create relationships between tables using primary and foreign keys.
-- Use ALTER TABLE statements to add at least 5 CHECK constraints across tables to restrict certain values, for example:
--   - Inserted dates must be greater than January 1, 2024
--   - Measured values must not be negative
--   - Specific fields can only accept predefined values
--   - Enforce uniqueness
--   - Enforce NOT NULL conditions
-- Assign meaningful names to your CHECK constraints.
-- Use appropriate data types for each column.
-- Apply DEFAULT values, STORED GENERATED columns, and GENERATED ALWAYS AS columns where necessary.

-- Create Employee table
DROP TABLE IF EXISTS  museum.Employee;
CREATE table if not exists museum.Employee (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    role VARCHAR(100),
    department VARCHAR(100) NOT NULL,
    hire_date DATE,
    contact_info text UNIQUE
);

-- Create Visitor table with Membership_Status CHECK constraint
DROP TABLE IF EXISTS  museum.Visitor;
CREATE table if not exists museum.Visitor (
    visitor_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    phone_number VARCHAR(20) UNIQUE,
    visit_date DATE,
    membership_status VARCHAR(50),
    CONSTRAINT membership_status_check CHECK (membership_status IN ('Member', 'Guest'))
);

-- Create Exhibition table
DROP TABLE IF EXISTS  museum.Exhibition;
CREATE table if not exists museum.Exhibition (
    exhibition_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    start_date DATE,
    end_date DATE,
    description TEXT,
    location VARCHAR(255),
    theme VARCHAR(100),
    employee_id INT,
    CONSTRAINT fk_exhibition_employee FOREIGN KEY (employee_id)
        REFERENCES museum.Employee(employee_id)
        ON DELETE SET NULL
);

-- Create Visitor_Exhibition table (Many-to-Many relationship)
drop table if exists museum.Visitor_Exhibition;
CREATE table if not exists museum.Visitor_Exhibition (
    visitor_id INT,
    exhibition_id INT,
    visit_date DATE,
    feedback TEXT,
    interaction_type VARCHAR(100),
    PRIMARY KEY (visitor_id, exhibition_id),
    CONSTRAINT fk_visitor FOREIGN KEY (visitor_id)
        REFERENCES museum.Visitor(visitor_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_exhibition FOREIGN KEY (exhibition_id)
        REFERENCES museum.Exhibition(exhibition_id)
        ON DELETE CASCADE
);

-- Create Artifact table with Condition CHECK constraint
drop table if exists museum.Artifact;
CREATE table if not exists museum.Artifact (
    artifact_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    date_acquired DATE,
    category VARCHAR(100),
    condition VARCHAR(50) DEFAULT 'Good',
    value DECIMAL(10,2) CHECK (value >= 0),
    artist VARCHAR(255),
    material_type VARCHAR(100),
    CONSTRAINT condition_check CHECK (condition IN ('New', 'Good', 'Fair', 'Damaged'))
);

-- Create Artifact_Exhibition table (Many-to-Many relationship)
drop table if exists museum.Artifact_Exhibition ;
CREATE table if not exists museum.Artifact_Exhibition (
    artifact_id INT,
    exhibition_id INT,
    display_order INT,
    PRIMARY KEY (artifact_id, exhibition_id),
    CONSTRAINT fk_artifact FOREIGN KEY (artifact_id)
        REFERENCES museum.Artifact(artifact_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_artifact_exhibition FOREIGN KEY (exhibition_id)
        REFERENCES museum.Exhibition(exhibition_id)
        ON DELETE CASCADE
);

-- Create Inventory table (One-to-One relationship with Artifact)
drop table if exists museum.Inventory ;
CREATE table if not exists museum.Inventory (
    inventory_id SERIAL PRIMARY KEY,
    artifact_id INT UNIQUE,
    storage_location VARCHAR(255) NOT NULL,
    quantity INT CHECK (quantity >= 0),
    date_updated DATE,
    CONSTRAINT fk_inventory_artifact FOREIGN KEY (artifact_id)
        REFERENCES museum.Artifact(artifact_id)
        ON DELETE CASCADE
);

-- Create additional constraints using ALTER TABLE

-- 1. Ensure that Visit_Date in Visitor table is after January 1, 2024
ALTER TABLE museum.Visitor
ADD CONSTRAINT check_visitor_date CHECK (visit_date > '2024-01-01');

-- 2. Ensure Value in Artifact table is non-negative (already included during creation for redundancy)
-- (no additional ALTER needed)

-- 3. Restrict Role in Employee table to specific allowed values
ALTER TABLE museum.Employee
ADD CONSTRAINT check_employee_role CHECK (role IN ('Curator', 'Security', 'Manager'));

-- 4. Enforce uniqueness of Phone_Number in Visitor table
-- (already declared during creation, so no additional ALTER needed)

-- 5. Department in Employee table must NOT be NULL
-- (already enforced during creation)

-- 6. Set default value for Condition column in Artifact table
-- (already set during creation)

-- 7. Add generated column for Artifact name length
ALTER TABLE museum.Artifact
ADD COLUMN name_length INT GENERATED ALWAYS AS (LENGTH(name)) STORED;

-- 8. Enforce valid Membership_Status values in Visitor table
-- (already added during creation)

-- 9. Ensure Storage_Location in Inventory is NOT NULL
-- (already enforced during creation)

-- 10. Ensure positive Quantity values in Inventory
-- (already included during creation)

-- 4. Populate tables with sample data
-- Ensure at least 6+ rows per table (totaling 36+ rows across all tables)
-- DML scripts should avoid specifying surrogate keys (auto-generated).
-- Default values should be used where applicable.

-- Insert data into Employee table
delete from museum.Employee;
INSERT INTO museum.Employee (first_name, last_name, role, department, hire_date, contact_info)
VALUES
('John', 'Doe', 'Curator', 'Art', '2024-02-15', 'johndoe@example.com'),
('Jane', 'Smith', 'Security', 'Security', '2024-03-20', 'janesmith@example.com'),
('Michael', 'Johnson', 'Manager', 'Management', '2024-01-25', 'michaeljohnson@example.com'),
('Sarah', 'Lee', 'Curator', 'Art', '2024-02-10', 'sarahlee@example.com'),
('David', 'Brown', 'Manager', 'Management', '2024-01-30', 'davidbrown@example.com'),
('Emily', 'Davis', 'Security', 'Security', '2024-03-15', 'emilydavis@example.com')
ON CONFLICT DO NOTHING;
select *
from museum.Employee;

-- Insert data into Visitor table
DELETE FROM museum.Visitor;
INSERT INTO museum.Visitor (first_name, last_name, email, phone_number, visit_date, membership_status)
VALUES
('Alice', 'Wang', 'alice.wang@example.com', '1234567890', '2024-03-25', 'Member'),
('Bob', 'Martin', 'bob.martin@example.com', '1234567891', '2024-02-12', 'Guest'),
('Charlie', 'Keller', 'charlie.keller@example.com', '1234567892', '2024-03-01', 'Member'),
('Diana', 'Moore', 'diana.moore@example.com', '1234567893', '2024-01-28', 'Guest'),
('Eve', 'Taylor', 'eve.taylor@example.com', '1234567894', '2024-02-05', 'Member'),
('Frank', 'Wilson', 'frank.wilson@example.com', '1234567895', '2024-03-10', 'Guest')
ON CONFLICT DO NOTHING;
select *
from museum.Visitor;
-- Insert data into Exhibition table
DELETE FROM museum.Exhibition;
INSERT INTO museum.Exhibition (title, start_date, end_date, description, location, theme, employee_id)
VALUES
('Renaissance Art', '2024-02-01', '2024-03-01', 'A collection of Renaissance paintings and sculptures.', 'Hall A', 'Art', 
  (SELECT employee_id FROM museum.Employee WHERE first_name = 'John' AND last_name = 'Doe')),
('Modern Photography', '2024-01-15', '2024-02-15', 'An exhibition showcasing modern photographers.', 'Hall B', 'Photography', 
  (SELECT employee_id FROM museum.Employee WHERE first_name = 'Jane' AND last_name = 'Smith')),
('Ancient Civilizations', '2024-03-01', '2024-04-01', 'Artifacts and exhibits from ancient civilizations.', 'Hall C', 'History', 
  (SELECT employee_id FROM museum.Employee WHERE first_name = 'Michael' AND last_name = 'Johnson')),
('Impressionism', '2024-01-05', '2024-02-05', 'Impressionist art movement exhibition.', 'Hall A', 'Art', 
  (SELECT employee_id FROM museum.Employee WHERE first_name = 'Sarah' AND last_name = 'Lee')),
('Futuristic Designs', '2024-03-15', '2024-04-15', 'Exploring futuristic art and design concepts.', 'Hall D', 'Design', 
  (SELECT employee_id FROM museum.Employee WHERE first_name = 'David' AND last_name = 'Brown')),
('Underwater Photography', '2024-02-20', '2024-03-20', 'Photography of underwater life and marine animals.', 'Hall B', 'Photography', 
  (SELECT employee_id FROM museum.Employee WHERE first_name = 'Emily' AND last_name = 'Davis'))
ON CONFLICT DO NOTHING;
select *
from museum.Exhibition;

-- Insert data into Visitor_Exhibition table
-- Insert data into Visitor_Exhibition table with additional conditions for uniqueness
DELETE FROM museum.Visitor_Exhibition;
INSERT INTO museum.Visitor_Exhibition (visitor_id, exhibition_id, visit_date, feedback, interaction_type)
VALUES
  -- Alice
  ((SELECT visitor_id FROM museum.Visitor 
    WHERE first_name = 'Alice' AND last_name = 'Wang' 
    AND email = 'alice.wang@example.com' 
    AND phone_number = '1234567890'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Renaissance Art' AND start_date = '2024-02-01' 
    AND location = 'Hall A'), 
   '2024-03-25', 'Great experience, very informative!', 'Guided Tour'),
  
  -- Bob
  ((SELECT visitor_id FROM museum.Visitor 
    WHERE first_name = 'Bob' AND last_name = 'Martin' 
    AND email = 'bob.martin@example.com' 
    AND phone_number = '1234567891'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Renaissance Art' AND start_date = '2024-02-01' 
    AND location = 'Hall A'), 
   '2024-02-12', 'Interesting exhibits, loved the paintings.', 'Self-guided'),
  
  -- Charlie
  ((SELECT visitor_id FROM museum.Visitor 
    WHERE first_name = 'Charlie' AND last_name = 'Keller' 
    AND email = 'charlie.keller@example.com' 
    AND phone_number = '1234567892'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Ancient Civilizations' AND start_date = '2024-03-01' 
    AND location = 'Hall C'), 
   '2024-03-01', 'Amazing artifacts, learned a lot!', 'Guided Tour'),
  
  -- Diana
  ((SELECT visitor_id FROM museum.Visitor 
    WHERE first_name = 'Diana' AND last_name = 'Moore' 
    AND email = 'diana.moore@example.com' 
    AND phone_number = '1234567893'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Modern Photography' AND start_date = '2024-01-15' 
    AND location = 'Hall B'), 
   '2024-01-28', 'The photographs were outstanding.', 'Self-guided'),
  
  -- Eve
  ((SELECT visitor_id FROM museum.Visitor 
    WHERE first_name = 'Eve' AND last_name = 'Taylor' 
    AND email = 'eve.taylor@example.com' 
    AND phone_number = '1234567894'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Impressionism' AND start_date = '2024-01-05' 
    AND location = 'Hall A'), 
   '2024-02-05', 'Impressionism is beautiful, loved the art.', 'Guided Tour'),
  
  -- Frank
  ((SELECT visitor_id FROM museum.Visitor 
    WHERE first_name = 'Frank' AND last_name = 'Wilson' 
    AND email = 'frank.wilson@example.com' 
    AND phone_number = '1234567895'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Futuristic Designs' AND start_date = '2024-03-15' 
    AND location = 'Hall D'), 
   '2024-03-10', 'Futuristic designs are thought-provoking.', 'Self-guided')
ON CONFLICT  DO NOTHING;
select *
from museum.Visitor_Exhibition;

-- Insert data into Artifact table
DELETE FROM museum.Artifact;
INSERT INTO museum.Artifact (name, description, date_acquired, category, condition, value, artist, material_type)
VALUES
('Mona Lisa', 'A famous painting by Leonardo da Vinci.', '2024-01-15', 'Painting', 'Good', 1000000.00, 'Leonardo da Vinci', 'Oil on Canvas'),
('Venus de Milo', 'A famous ancient Greek statue.', '2024-02-10', 'Sculpture', 'Fair', 2000000.00, 'Unknown', 'Marble'),
('The Thinker', 'A bronze sculpture by Auguste Rodin.', '2024-03-05', 'Sculpture', 'Good', 1500000.00, 'Auguste Rodin', 'Bronze'),
('Starry Night', 'A painting by Vincent van Gogh.', '2024-03-01', 'Painting', 'New', 3000000.00, 'Vincent van Gogh', 'Oil on Canvas'),
('The Scream', 'A famous painting by Edvard Munch.', '2024-02-25', 'Painting', 'Damaged', 500000.00, 'Edvard Munch', 'Oil on Canvas'),
('The Persistence of Memory', 'A painting by Salvador Dalí.', '2024-01-10', 'Painting', 'Good', 2500000.00, 'Salvador Dalí', 'Oil on Canvas')
on conflict  do nothing;
select *
from museum.Artifact;


-- Insert data into Artifact_Exhibition table
-- Insert data into Artifact_Exhibition table with additional conditions for uniqueness
DELETE FROM museum.Artifact_Exhibition;
INSERT INTO museum.Artifact_Exhibition (artifact_id, exhibition_id, display_order)
VALUES
  -- Mona Lisa
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'Mona Lisa' AND artist = 'Leonardo da Vinci'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Renaissance Art' AND start_date = '2024-02-01' 
    AND location = 'Hall A'), 
   1),
  
  -- Venus de Milo
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'Venus de Milo' AND artist = 'Unknown'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Renaissance Art' AND start_date = '2024-02-01' 
    AND location = 'Hall A'), 
   2),
  
  -- The Thinker
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'The Thinker' AND artist = 'Auguste Rodin'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Ancient Civilizations' AND start_date = '2024-03-01' 
    AND location = 'Hall C'), 
   1),
  
  -- Starry Night
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'Starry Night' AND artist = 'Vincent van Gogh'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Impressionism' AND start_date = '2024-01-05' 
    AND location = 'Hall A'), 
   1),
  
  -- The Scream
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'The Scream' AND artist = 'Edvard Munch'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Modern Photography' AND start_date = '2024-01-15' 
    AND location = 'Hall B'), 
   1),
  
  -- The Persistence of Memory
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'The Persistence of Memory' AND artist = 'Salvador Dalí'), 
   (SELECT exhibition_id FROM museum.Exhibition 
    WHERE title = 'Futuristic Designs' AND start_date = '2024-03-15' 
    AND location = 'Hall D'), 
   1)
ON CONFLICT (artifact_id, exhibition_id) DO NOTHING;

select *
from museum.Artifact_Exhibition;
-- Insert data into Inventory table with additional conditions for uniqueness
DELETE FROM museum.Inventory;
INSERT INTO museum.Inventory (artifact_id, storage_location, quantity, date_updated)
VALUES
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'Mona Lisa' AND artist = 'Leonardo da Vinci'), 'Storage Room A', 5, '2024-03-20'),
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'Venus de Milo' AND artist = 'Unknown'), 'Storage Room B', 3, '2024-03-18'),
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'The Thinker' AND artist = 'Auguste Rodin'), 'Storage Room A', 4, '2024-03-19'),
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'Starry Night' AND artist = 'Vincent van Gogh'), 'Storage Room C', 2, '2024-03-20'),
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'The Scream' AND artist = 'Edvard Munch'), 'Storage Room D', 1, '2024-03-21'),
  ((SELECT artifact_id FROM museum.Artifact 
    WHERE name = 'The Persistence of Memory' AND artist = 'Salvador Dalí'), 'Storage Room C', 6, '2024-03-17')
ON CONFLICT (artifact_id) DO NOTHING;

select *
from museum.Inventory;

-- 5. Create functions

-- 5.1 Create a function to update data in a specified table
DROP FUNCTION IF EXISTS museum.update_table_data;

CREATE OR REPLACE FUNCTION museum.update_table_data(
    primary_key_value INT,
    column_name TEXT,
    new_value TEXT
)
RETURNS VOID AS $$
DECLARE
    dynamic_sql TEXT;
BEGIN
    dynamic_sql := 'UPDATE museum.Visitor SET ' || column_name || ' = $1 WHERE visitor_id = $2';
    EXECUTE dynamic_sql USING new_value, primary_key_value;
    RAISE NOTICE 'Updated % in Visitor where visitor_id = %', column_name, primary_key_value;
END;
$$ LANGUAGE plpgsql;

-- Example call:
-- SELECT museum.update_table_data(1, 'email', 'newemail@example.com');

-- 5.2 Create a function to add a new transaction
DROP TABLE IF EXISTS museum.Transaction;

CREATE table if not exists museum.Transaction (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date DATE DEFAULT CURRENT_DATE,
    transaction_type VARCHAR(100),
    amount DECIMAL(10,2),
    visitor_id INT,
    exhibition_id INT,
    employee_id INT,
    CONSTRAINT transaction_unique_key UNIQUE (visitor_id, exhibition_id, transaction_date, transaction_type),
    FOREIGN KEY (visitor_id) REFERENCES museum.Visitor(visitor_id),
    FOREIGN KEY (exhibition_id) REFERENCES museum.Exhibition(exhibition_id),
    FOREIGN KEY (employee_id) REFERENCES museum.Employee(employee_id)
);

DROP FUNCTION IF EXISTS museum.add_transaction;
CREATE OR REPLACE FUNCTION museum.add_transaction(
    p_visitor_id INT,
    p_exhibition_id INT,
    p_transaction_date DATE,
    p_transaction_type VARCHAR(100),
    p_amount DECIMAL(10,2),
    p_employee_id INT
)
RETURNS VOID AS
$$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM museum.Transaction
        WHERE visitor_id = p_visitor_id
          AND exhibition_id = p_exhibition_id
          AND transaction_date = p_transaction_date
          AND transaction_type = p_transaction_type
    ) THEN
        RAISE EXCEPTION 'Transaction already exists for this visitor, exhibition, date, and type.';
    END IF;

    INSERT INTO museum.Transaction (
        transaction_date,
        transaction_type,
        amount,
        visitor_id,
        exhibition_id,
        employee_id
    ) VALUES (
        p_transaction_date,
        p_transaction_type,
        p_amount,
        p_visitor_id,
        p_exhibition_id,
        p_employee_id
    );

    RAISE NOTICE 'Transaction inserted successfully.';
END;
$$ LANGUAGE plpgsql;

-- 6. Create a view presenting analytics for the most recently added quarter (WIP - Not yet complete)

drop view if exists museum.recent_quarter_analytics;
CREATE OR REPLACE VIEW museum.recent_quarter_analytics AS
WITH recent_quarter AS (
    -- Determine the most recent quarter based on the latest transaction date
    SELECT
        EXTRACT(YEAR FROM MAX(transaction_date)) AS year,
        EXTRACT(QUARTER FROM MAX(transaction_date)) AS quarter
    FROM museum.Transaction
),
quarter_data AS (
    -- Filter all transactions that belong to the most recent quarter
    SELECT 
        t.transaction_date,
        t.transaction_type,
        t.amount,
        v.first_name || ' ' || v.last_name AS visitor_name,
        e.title AS exhibition_title,
        e.start_date AS exhibition_start_date,
        e.end_date AS exhibition_end_date
    FROM museum.Transaction t
    JOIN museum.Visitor v ON t.visitor_id = v.visitor_id
    JOIN museum.Exhibition e ON t.exhibition_id = e.exhibition_id
    WHERE EXTRACT(YEAR FROM t.transaction_date) = (SELECT year FROM recent_quarter)
    AND EXTRACT(QUARTER FROM t.transaction_date) = (SELECT quarter FROM recent_quarter)
)
-- Analytics for the most recent quarter
SELECT
    COUNT(DISTINCT transaction_date) AS total_transactions,  -- Total number of transactions
    SUM(amount) AS total_revenue,  -- Total revenue in the quarter
    COUNT(DISTINCT visitor_name) AS total_visitors,  -- Number of distinct visitors
    COUNT(DISTINCT exhibition_title) AS total_exhibitions -- Number of distinct exhibitions
FROM quarter_data;

SELECT * FROM museum.recent_quarter_analytics;

--7. Create a read-only role for the manager. This role should have permission to perform SELECT queries on the database tables, and also be able to log in. Please ensure that you adhere to best practices for database security when defining this role

-- Step 1: Create the read-only role if it doesn't already exist
DO $$
BEGIN
    -- Check if the role already exists, and create it if necessary
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'manager_read_only') THEN
        CREATE ROLE manager_read_only WITH LOGIN PASSWORD 'securepassword';  -- Make sure to replace 'securepassword' with a strong, real password.
    END IF;
END $$;

-- Step 2: Grant the role permission to connect to the database, but only if it hasn’t already been granted
DO $$
BEGIN
    -- If the role exists, grant them connect permission
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'manager_read_only') THEN
        RAISE NOTICE 'Role manager_read_only does not exist, skipping CONNECT privilege.';
    ELSE
        GRANT CONNECT ON DATABASE museum TO manager_read_only;
    END IF;
END $$;

-- Step 3: Give the role permission to use the museum schema (so they can access the tables), if not granted yet
DO $$
BEGIN
    -- Check if the role already has the permission to use the schema, and grant it if needed
    IF NOT EXISTS (SELECT 1 FROM information_schema.role_table_grants WHERE grantee = 'manager_read_only' AND table_schema = 'museum') THEN
        GRANT USAGE ON SCHEMA museum TO manager_read_only;
    END IF;
END $$;

-- Step 4: Grant read-only access to all tables in the museum schema, but only if it hasn't been granted already
DO $$
BEGIN
    -- If the role doesn't already have SELECT permissions, grant it
    IF NOT EXISTS (SELECT 1 FROM information_schema.role_table_grants WHERE grantee = 'manager_read_only' AND table_schema = 'museum' AND privilege_type = 'SELECT') THEN
        GRANT SELECT ON ALL TABLES IN SCHEMA museum TO manager_read_only;
    END IF;
END $$;

-- Step 5: Optionally, give them permission to view sequence values (if applicable), but only if they don’t already have it
DO $$
BEGIN
    -- Check if they already have access to sequences and grant permission if not
    IF NOT EXISTS (SELECT 1 FROM information_schema.role_table_grants WHERE grantee = 'manager_read_only' AND table_schema = 'museum' AND privilege_type = 'USAGE') THEN
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA museum TO manager_read_only;
    END IF;
END $$;

-- Step 6: Ensure that the role will automatically get read-only access to any future tables created in the museum schema
DO $$
BEGIN
    -- Check if the default privilege for SELECT on tables exists and set it if necessary
    IF NOT EXISTS (SELECT 1 FROM pg_default_acl WHERE defaclrole = (SELECT oid FROM pg_roles WHERE rolname = 'manager_read_only') AND defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'museum') AND defaclobjtype = 'r') THEN
        ALTER DEFAULT PRIVILEGES IN SCHEMA museum GRANT SELECT ON TABLES TO manager_read_only;
    END IF;
END $$;

-- Step 7: Clean up by ensuring the role doesn't have any unnecessary privileges, like modifying data
DO $$
BEGIN
    -- Remove any other privileges the role might have that we don't want them to have
    REVOKE ALL ON DATABASE museum FROM manager_read_only;  -- Make sure they don’t have extra access to the database.
    REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA museum FROM manager_read_only;  -- They shouldn't be able to modify or delete data.
END $$;
