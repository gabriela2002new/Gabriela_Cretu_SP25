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
CREATE TABLE public.Employee (
    employee_id SERIAL PRIMARY KEY,
    First_Name VARCHAR(100),
    Last_Name VARCHAR(100),
    Role VARCHAR(100),
    Department VARCHAR(100) NOT NULL,
    Hire_Date DATE,
    Contact_Info TEXT
);

-- Create Visitor table with Membership_Status CHECK constraint
CREATE TABLE public.Visitor (
    visitor_id SERIAL PRIMARY KEY,
    First_Name VARCHAR(100),
    Last_Name VARCHAR(100),
    Email VARCHAR(100) UNIQUE,
    Phone_Number VARCHAR(20) UNIQUE,
    Visit_Date DATE,
    Membership_Status VARCHAR(50),
    CONSTRAINT membership_status_check CHECK (Membership_Status IN ('Member', 'Guest'))
);

-- Create Exhibition table
CREATE TABLE public.Exhibition (
    exhibition_id SERIAL PRIMARY KEY,
    Title VARCHAR(255),
    Start_Date DATE,
    End_Date DATE,
    Description TEXT,
    Location VARCHAR(255),
    Theme VARCHAR(100),
    employee_id INT,
    CONSTRAINT fk_exhibition_employee FOREIGN KEY (employee_id)
        REFERENCES public.Employee(employee_id)
        ON DELETE SET NULL
);

-- Create Visitor_Exhibition table (Many-to-Many relationship)
CREATE TABLE public.Visitor_Exhibition (
    visitor_id INT,
    exhibition_id INT,
    Visit_Date DATE,
    Feedback TEXT,
    Interaction_Type VARCHAR(100),
    PRIMARY KEY (visitor_id, exhibition_id),
    CONSTRAINT fk_visitor FOREIGN KEY (visitor_id)
        REFERENCES public.Visitor(visitor_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_exhibition FOREIGN KEY (exhibition_id)
        REFERENCES public.Exhibition(exhibition_id)
        ON DELETE CASCADE
);

-- Create Artifact table with Condition CHECK constraint
CREATE TABLE public.Artifact (
    artifact_id SERIAL PRIMARY KEY,
    Name VARCHAR(255),
    Description TEXT,
    Date_Acquired DATE,
    Category VARCHAR(100),
    Condition VARCHAR(50) DEFAULT 'Good',
    Value DECIMAL(10,2) CHECK (Value >= 0),
    Artist VARCHAR(255),
    Material_Type VARCHAR(100),
    CONSTRAINT condition_check CHECK (Condition IN ('New', 'Good', 'Fair', 'Damaged'))
);

-- Create Artifact_Exhibition table (Many-to-Many relationship)
CREATE TABLE public.Artifact_Exhibition (
    artifact_id INT,
    exhibition_id INT,
    display_Order INT,
    PRIMARY KEY (artifact_id, exhibition_id),
    CONSTRAINT fk_artifact FOREIGN KEY (artifact_id)
        REFERENCES public.Artifact(artifact_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_artifact_exhibition FOREIGN KEY (exhibition_id)
        REFERENCES public.Exhibition(exhibition_id)
        ON DELETE CASCADE
);

-- Create Inventory table (One-to-One relationship with Artifact)
CREATE TABLE public.Inventory (
    inventory_id SERIAL PRIMARY KEY,
    artifact_id INT UNIQUE,
    Storage_Location VARCHAR(255) NOT NULL,
    Quantity INT CHECK (Quantity >= 0),
    Date_Updated DATE,
    CONSTRAINT fk_inventory_artifact FOREIGN KEY (artifact_id)
        REFERENCES public.Artifact(artifact_id)
        ON DELETE CASCADE
);

-- Create additional constraints using ALTER TABLE

-- 1. Ensure that Visit_Date in Visitor table is after January 1, 2024
ALTER TABLE public.Visitor
ADD CONSTRAINT check_visitor_date CHECK (Visit_Date > '2024-01-01');

-- 2. Ensure Value in Artifact table is non-negative (already included during creation for redundancy)
-- (no additional ALTER needed)

-- 3. Restrict Role in Employee table to specific allowed values
ALTER TABLE public.Employee
ADD CONSTRAINT check_employee_role CHECK (Role IN ('Curator', 'Security', 'Manager'));

-- 4. Enforce uniqueness of Phone_Number in Visitor table
-- (already declared during creation, so no additional ALTER needed)

-- 5. Department in Employee table must NOT be NULL
-- (already enforced during creation)

-- 6. Set default value for Condition column in Artifact table
-- (already set during creation)

-- 7. Add generated column for Artifact name length
ALTER TABLE public.Artifact
ADD COLUMN Name_Length INT GENERATED ALWAYS AS (LENGTH(Name)) STORED;

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
INSERT INTO public.Employee (First_Name, Last_Name, Role, Department, Hire_Date, Contact_Info)
VALUES
('John', 'Doe', 'Curator', 'Art', '2024-02-15', 'johndoe@example.com'),
('Jane', 'Smith', 'Security', 'Security', '2024-03-20', 'janesmith@example.com'),
('Michael', 'Johnson', 'Manager', 'Management', '2024-01-25', 'michaeljohnson@example.com'),
('Sarah', 'Lee', 'Curator', 'Art', '2024-02-10', 'sarahlee@example.com'),
('David', 'Brown', 'Manager', 'Management', '2024-01-30', 'davidbrown@example.com'),
('Emily', 'Davis', 'Security', 'Security', '2024-03-15', 'emilydavis@example.com');

-- Insert data into Visitor table
INSERT INTO public.Visitor (First_Name, Last_Name, Email, Phone_Number, Visit_Date, Membership_Status)
VALUES
('Alice', 'Wang', 'alice.wang@example.com', '1234567890', '2024-03-25', 'Member'),
('Bob', 'Martin', 'bob.martin@example.com', '1234567891', '2024-02-12', 'Guest'),
('Charlie', 'Keller', 'charlie.keller@example.com', '1234567892', '2024-03-01', 'Member'),
('Diana', 'Moore', 'diana.moore@example.com', '1234567893', '2024-01-28', 'Guest'),
('Eve', 'Taylor', 'eve.taylor@example.com', '1234567894', '2024-02-05', 'Member'),
('Frank', 'Wilson', 'frank.wilson@example.com', '1234567895', '2024-03-10', 'Guest');

-- Insert data into Exhibition table
INSERT INTO public.Exhibition (Title, Start_Date, End_Date, Description, Location, Theme, employee_id)
VALUES
('Renaissance Art', '2024-02-01', '2024-03-01', 'A collection of Renaissance paintings and sculptures.', 'Hall A', 'Art', 1),
('Modern Photography', '2024-01-15', '2024-02-15', 'An exhibition showcasing modern photographers.', 'Hall B', 'Photography', 2),
('Ancient Civilizations', '2024-03-01', '2024-04-01', 'Artifacts and exhibits from ancient civilizations.', 'Hall C', 'History', 3),
('Impressionism', '2024-01-05', '2024-02-05', 'Impressionist art movement exhibition.', 'Hall A', 'Art', 4),
('Futuristic Designs', '2024-03-15', '2024-04-15', 'Exploring futuristic art and design concepts.', 'Hall D', 'Design', 5),
('Underwater Photography', '2024-02-20', '2024-03-20', 'Photography of underwater life and marine animals.', 'Hall B', 'Photography', 6);

-- Insert data into Visitor_Exhibition table
INSERT INTO public.Visitor_Exhibition (visitor_id, exhibition_id, Visit_Date, Feedback, Interaction_Type)
VALUES
(1, 1, '2024-03-25', 'Great experience, very informative!', 'Guided Tour'),
(2, 1, '2024-02-12', 'Interesting exhibits, loved the paintings.', 'Self-guided'),
(3, 3, '2024-03-01', 'Amazing artifacts, learned a lot!', 'Guided Tour'),
(4, 2, '2024-01-28', 'The photographs were outstanding.', 'Self-guided'),
(5, 4, '2024-02-05', 'Impressionism is beautiful, loved the art.', 'Guided Tour'),
(6, 5, '2024-03-10', 'Futuristic designs are thought-provoking.', 'Self-guided');

-- Insert data into Artifact table
INSERT INTO public.Artifact (Name, Description, Date_Acquired, Category, Condition, Value, Artist, Material_Type)
VALUES
('Mona Lisa', 'A famous painting by Leonardo da Vinci.', '2024-01-15', 'Painting', 'Good', 1000000.00, 'Leonardo da Vinci', 'Oil on Canvas'),
('Venus de Milo', 'A famous ancient Greek statue.', '2024-02-10', 'Sculpture', 'Fair', 2000000.00, 'Unknown', 'Marble'),
('The Thinker', 'A bronze sculpture by Auguste Rodin.', '2024-03-05', 'Sculpture', 'Good', 1500000.00, 'Auguste Rodin', 'Bronze'),
('Starry Night', 'A painting by Vincent van Gogh.', '2024-03-01', 'Painting', 'New', 3000000.00, 'Vincent van Gogh', 'Oil on Canvas'),
('The Scream', 'A famous painting by Edvard Munch.', '2024-02-25', 'Painting', 'Damaged', 500000.00, 'Edvard Munch', 'Oil on Canvas'),
('The Persistence of Memory', 'A painting by Salvador Dalí.', '2024-01-10', 'Painting', 'Good', 2500000.00, 'Salvador Dalí', 'Oil on Canvas');

-- Insert data into Artifact_Exhibition table
INSERT INTO public.Artifact_Exhibition (artifact_id, exhibition_id, display_Order)
VALUES
(1, 1, 1),
(2, 1, 2),
(3, 3, 1),
(4, 4, 1),
(5, 2, 1),
(6, 5, 1);

-- Insert data into Inventory table
INSERT INTO public.Inventory (artifact_id, Storage_Location, Quantity, Date_Updated)
VALUES
(1, 'Storage Room A', 5, '2024-03-20'),
(2, 'Storage Room B', 3, '2024-03-18'),
(3, 'Storage Room A', 4, '2024-03-19'),
(4, 'Storage Room C', 2, '2024-03-20'),
(5, 'Storage Room D', 1, '2024-03-21'),
(6, 'Storage Room C', 6, '2024-03-17');

-- 5. Create functions

-- 5.1 Create a function to update data in a specified table
CREATE OR REPLACE FUNCTION public.update_table_data(
    primary_key_value INT,
    column_name TEXT,
    new_value TEXT
)
RETURNS VOID AS $$
DECLARE
    dynamic_sql TEXT;
BEGIN
    dynamic_sql := 'UPDATE public.Visitor SET ' || column_name || ' = $1 WHERE visitor_id = $2';
    EXECUTE dynamic_sql USING new_value, primary_key_value;
    RAISE NOTICE 'Updated % in Visitor where visitor_id = %', column_name, primary_key_value;
END;
$$ LANGUAGE plpgsql;

-- Example call:
-- SELECT public.update_table_data(1, 'email', 'newemail@example.com');

-- 5.2 Create a function to add a new transaction
DROP TABLE IF EXISTS public.Transaction;

CREATE TABLE public.Transaction (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date DATE DEFAULT CURRENT_DATE,
    transaction_type VARCHAR(100),
    amount DECIMAL(10,2),
    visitor_id INT,
    exhibition_id INT,
    employee_id INT,
    CONSTRAINT transaction_unique_key UNIQUE (visitor_id, exhibition_id, transaction_date, transaction_type),
    FOREIGN KEY (visitor_id) REFERENCES public.Visitor(visitor_id),
    FOREIGN KEY (exhibition_id) REFERENCES public.Exhibition(exhibition_id),
    FOREIGN KEY (employee_id) REFERENCES public.Employee(employee_id)
);

CREATE OR REPLACE FUNCTION public.add_transaction(
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
        FROM public.Transaction
        WHERE visitor_id = p_visitor_id
          AND exhibition_id = p_exhibition_id
          AND transaction_date = p_transaction_date
          AND transaction_type = p_transaction_type
    ) THEN
        RAISE EXCEPTION 'Transaction already exists for this visitor, exhibition, date, and type.';
    END IF;

    INSERT INTO public.Transaction (
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


CREATE OR REPLACE VIEW public.recent_quarter_analytics AS
WITH recent_quarter AS (
    -- Determine the most recent quarter based on the latest transaction date
    SELECT
        EXTRACT(YEAR FROM MAX(transaction_date)) AS year,
        EXTRACT(QUARTER FROM MAX(transaction_date)) AS quarter
    FROM public.Transaction
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
    FROM public.Transaction t
    JOIN public.Visitor v ON t.visitor_id = v.visitor_id
    JOIN public.Exhibition e ON t.exhibition_id = e.exhibition_id
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

SELECT * FROM public.recent_quarter_analytics;

--7. Create a read-only role for the manager. This role should have permission to perform SELECT queries on the database tables, and also be able to log in. Please ensure that you adhere to best practices for database security when defining this role

-- Step 1: Create the read-only role
CREATE ROLE manager_read_only WITH LOGIN PASSWORD 'securepassword';  -- Replace 'securepassword' with a real secure password.

-- Step 2: Grant CONNECT privilege to allow the role to log in
GRANT CONNECT ON DATABASE museum TO manager_read_only;

-- Step 3: Grant USAGE on the public schema (to access the tables within it)
GRANT USAGE ON SCHEMA public TO manager_read_only;

-- Step 4: Grant SELECT privilege on all tables in the public schema (for read-only access)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO manager_read_only;

-- Step 5: Grant SELECT privilege on sequences if you want the manager to read sequence values (optional)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO manager_read_only;

-- Step 6: Make sure that the manager_read_only role automatically has SELECT permission on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO manager_read_only;

-- Step 7: Optionally, revoke any other unnecessary privileges from the role
REVOKE ALL ON DATABASE museum FROM manager_read_only;  -- Ensure no other privileges are granted to the role by default.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA public FROM manager_read_only;  -- Ensure they can’t modify data.
