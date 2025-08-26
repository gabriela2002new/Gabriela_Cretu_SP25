-- ======================================================================
-- ETL Workflow: Redshift Star Schema for dilab_student43
-- ======================================================================

-- ======================================================================
-- CREATING THE REPORT
-- ======================================================================

-- ======================================================================
-- Task 2
-- ======================================================================


-- a) You could load all the tables, but because of limited resources please load only tables that are needed to create a report for your customer 
--(at least 3 tables should be involved). It can be some tables that will be used in aggregations and calculations of KPIs. The business meaning of the report 
--is up to you and your creativity.


-- login & Environment Setup
SELECT current_user;
SELECT current_database();
CREATE SCHEMA IF NOT EXISTS dilab_student43;

-- drop Existing Objects (if any)
CREATE OR REPLACE PROCEDURE dilab_student43.drop_user_dilab_student43_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS dilab_student43.fct_sales CASCADE;
    DROP TABLE IF EXISTS dilab_student43.dim_payment_methods CASCADE;
    DROP TABLE IF EXISTS dilab_student43.dim_employees CASCADE;
    DROP TABLE IF EXISTS dilab_student43.dim_customers_scd CASCADE;
    DROP TABLE IF EXISTS dilab_student43.dim_retailer_license_numbers CASCADE;
    DROP TABLE IF EXISTS dilab_student43.dim_game_numbers CASCADE;
    RAISE NOTICE 'All DM objects dropped successfully.';
END;
$$;
CALL dilab_student43.drop_user_dilab_student43_objects();

-- create tables
CREATE OR REPLACE PROCEDURE dilab_student43.create_user_dilab_student43_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Dimension tables
    CREATE TABLE IF NOT EXISTS dilab_student43.dim_game_numbers (
        game_number_surr_id BIGINT PRIMARY KEY,
        game_number_src_id VARCHAR(255) UNIQUE,
        game_number_name VARCHAR(100),
        game_category_id BIGINT,
        game_category_name VARCHAR(100),
        game_type_id BIGINT,
        game_type_name VARCHAR(100),
        draw_dt DATE,
        average_odds VARCHAR(30),
        average_odds_prob FLOAT,
        mid_tier_prize FLOAT,
        top_tier_prize FLOAT,
        small_prize FLOAT,
        winning_chance FLOAT,
        winning_jackpot FLOAT,
        source_system VARCHAR(30),
        source_entity VARCHAR(30),
        insert_dt DATE,
        update_dt DATE
    );

    CREATE TABLE IF NOT EXISTS dilab_student43.dim_retailer_license_numbers (
        retailer_license_number_surr_id BIGINT PRIMARY KEY,
        retailer_license_number_src_id VARCHAR(255) UNIQUE,
        retailer_license_number_name VARCHAR(100),
        retailer_location_name_id BIGINT,
        retailer_location_name VARCHAR(100),
        zip_id BIGINT,
        zip_name VARCHAR(100),
        city_id BIGINT,
        city_name VARCHAR(100),
        state_id BIGINT,
        state_name VARCHAR(100),
        source_system VARCHAR(30),
        source_entity VARCHAR(30),
        insert_dt DATE,
        update_dt DATE
    );

    CREATE TABLE IF NOT EXISTS dilab_student43.dim_customers_scd (
        customer_surr_id BIGINT PRIMARY KEY,
        customer_src_id VARCHAR(255),
        customer_name VARCHAR(100),
        customer_registration_dt DATE,
        zip_id BIGINT,
        zip_name VARCHAR(100),
        city_id BIGINT,
        city_name VARCHAR(100),
        state_id BIGINT,
        state_name VARCHAR(100),
        customer_gender VARCHAR(20),
        customer_dob DATE,
        customer_email VARCHAR(100),
        customer_phone VARCHAR(50),
        source_system VARCHAR(30),
        source_entity VARCHAR(30),
        insert_dt DATE,
        start_dt DATE,
        end_dt DATE,
        is_active BOOLEAN,
        CONSTRAINT uq_customer_src_start UNIQUE (customer_src_id, start_dt)
    );

    CREATE TABLE IF NOT EXISTS dilab_student43.dim_employees (
        employee_surr_id BIGINT PRIMARY KEY,
        employee_src_id VARCHAR(255) UNIQUE,
        employee_name VARCHAR(100),
        employee_hire_dt DATE,
        employee_status_id BIGINT,
        employee_status_name VARCHAR(100),
        employee_department_id BIGINT,
        employee_department_name VARCHAR(100),
        employee_email VARCHAR(100),
        employee_phone VARCHAR(50),
        employee_salary FLOAT,
        source_system VARCHAR(30),
        source_entity VARCHAR(30),
        insert_dt DATE,
        update_dt DATE
    );

    CREATE TABLE IF NOT EXISTS dilab_student43.dim_payment_methods (
        payment_method_surr_id BIGINT PRIMARY KEY,
        payment_method_src_id VARCHAR(255) UNIQUE,
        payment_method_name VARCHAR(100),
        source_system VARCHAR(30),
        source_entity VARCHAR(30),
        insert_dt DATE,
        update_dt DATE
    );

    -- Fact table
    CREATE TABLE IF NOT EXISTS dilab_student43.fct_sales (
        game_number_surr_id BIGINT REFERENCES dilab_student43.dim_game_numbers(game_number_surr_id),
        customer_surr_id BIGINT REFERENCES dilab_student43.dim_customers_scd(customer_surr_id),
        employee_surr_id BIGINT REFERENCES dilab_student43.dim_employees(employee_surr_id),
        retailer_license_number_surr_id BIGINT REFERENCES dilab_student43.dim_retailer_license_numbers(retailer_license_number_surr_id),
        payment_method_surr_id BIGINT REFERENCES dilab_student43.dim_payment_methods(payment_method_surr_id),
        event_dt DATE NOT NULL,
        ticket_price FLOAT,
        tickets_bought INT,
        payout FLOAT,
        sales FLOAT,
        insert_dt DATE,
        update_dt DATE,
        CONSTRAINT fct_sales_grain_unique UNIQUE (
            game_number_surr_id,
            customer_surr_id,
            employee_surr_id,
            retailer_license_number_surr_id,
            payment_method_surr_id,
            event_dt
        )
    );

    RAISE NOTICE 'All DM objects created successfully.';
END;
$$;
CALL dilab_student43.create_user_dilab_student43_objects();

-- load Data from S3
CREATE OR REPLACE PROCEDURE dilab_student43.load_user_dilab_student43_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Loading dim_game_numbers...';
    COPY dilab_student43.dim_game_numbers
    FROM 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/dim_game_numbers/dim_game_numbers.csv'
    IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
    CSV IGNOREHEADER 1 DELIMITER ',';

    RAISE NOTICE 'Loading dim_customers_scd...';
    COPY dilab_student43.dim_customers_scd
    FROM 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/dim_customers_scd/dim_customers_scd.csv'
    IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
    CSV IGNOREHEADER 1 DELIMITER ',';

    RAISE NOTICE 'Loading dim_employees...';
    COPY dilab_student43.dim_employees
    FROM 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/dim_employees/dim_employees.csv'
    IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
    CSV IGNOREHEADER 1 DELIMITER ',';

    RAISE NOTICE 'Loading dim_payment_methods...';
    COPY dilab_student43.dim_payment_methods
    FROM 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/dim_payment_methods/dim_payment_methods.csv'
    IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
    CSV IGNOREHEADER 1 DELIMITER ',';

    RAISE NOTICE 'Loading dim_retailer_license_numbers...';
    COPY dilab_student43.dim_retailer_license_numbers
    FROM 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/dim_retailer_license_numbers/dim_retailer_license_numbers.csv'
    IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
    CSV IGNOREHEADER 1 DELIMITER ',';

    RAISE NOTICE 'Loading fct_sales...';
    COPY dilab_student43.fct_sales
    FROM 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales/fct_sales.csv'
    IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
    CSV IGNOREHEADER 1 DELIMITER ',';

    RAISE NOTICE 'All tables loaded successfully.';
END;
$$;
CALL dilab_student43.load_user_dilab_student43_objects();


SELECT * FROM dilab_student43.fct_sales LIMIT 10;


--b) After loading your tables check its initial compression types, distribution style, sort keys. Make a description of your analysis.
SELECT 
    "schema",
    "table",
    size AS size_mb,
    diststyle,
    sortkey1 AS sort_keys
FROM svv_table_info
WHERE "schema" = 'dilab_student43';

ANALYZE COMPRESSION dilab_student43.dim_game_numbers;
ANALYZE COMPRESSION dilab_student43.dim_customers_scd;
ANALYZE COMPRESSION dilab_student43.dim_employees;
ANALYZE COMPRESSION dilab_student43.dim_payment_methods;
ANALYZE COMPRESSION dilab_student43.dim_retailer_license_numbers;
ANALYZE COMPRESSION dilab_student43.fct_sales;

analyze
-- ======================================================================
-- Task 3
-- ======================================================================

--a) Identify compression types (encoding) of each column of this table (YOUR_TABLE_defaultcomp)
ANALYZE COMPRESSION dilab_student43.dim_game_numbers;

--b)Create table YOUR_TABLE_withoutcomp with similar to YOUR_TABLE_defaultcomp columns/data types, but without any compression applied and put there the 
--same data as in the YOUR_TABLE_defaultcomp table.
DROP TABLE IF EXISTS dilab_student43.dim_game_numbers_nocomp;

CREATE TABLE dilab_student43.dim_game_numbers_nocomp
(
    game_number_surr_id BIGINT ENCODE RAW,
    game_number_src_id VARCHAR(255) ENCODE RAW,
    game_number_name VARCHAR(100) ENCODE RAW,
    game_category_id BIGINT ENCODE RAW,
    game_category_name VARCHAR(100) ENCODE RAW,
    game_type_id BIGINT ENCODE RAW,
    game_type_name VARCHAR(100) ENCODE RAW,
    draw_dt DATE ENCODE RAW,
    average_odds VARCHAR(30) ENCODE RAW,
    average_odds_prob FLOAT ENCODE RAW,
    mid_tier_prize FLOAT ENCODE RAW,
    top_tier_prize FLOAT ENCODE RAW,
    small_prize FLOAT ENCODE RAW,
    winning_chance FLOAT ENCODE RAW,
    winning_jackpot FLOAT ENCODE RAW,
    source_system VARCHAR(30) ENCODE RAW,
    source_entity VARCHAR(30) ENCODE RAW,
    insert_dt DATE ENCODE RAW,
    update_dt DATE ENCODE RAW
);

-- Load data from the original table
INSERT INTO dilab_student43.dim_game_numbers_nocomp
SELECT * FROM dilab_student43.dim_game_numbers;


--c)Use analyze command (on YOUR_TABLE_defaultcomp or YOUR_TABLE_withoutcomp table) to identify best compression methods suggested by Redshift.Create
-- a table YOUR_TABLE_analyzedcomp (same columns but applying recommended encoding types and put same data as in the YOUR_TABLE_defaultcomp table there).


-- analyze compression for the no-compression table
ANALYZE COMPRESSION dilab_student43.dim_game_numbers_nocomp;

--create table with column-level compression recommendations
DROP TABLE IF EXISTS dilab_student43.dim_game_numbers_analyzedcomp;

CREATE TABLE dilab_student43.dim_game_numbers_analyzedcomp
(
    game_number_surr_id BIGINT ENCODE RAW,
    game_number_src_id VARCHAR(255) ENCODE RAW,
    game_number_name VARCHAR(100) ENCODE ZSTD,
    game_category_id BIGINT ENCODE RAW,
    game_category_name VARCHAR(100) ENCODE ZSTD,
    game_type_id BIGINT ENCODE RAW,
    game_type_name VARCHAR(100) ENCODE RAW,
    draw_dt DATE ENCODE RAW,
    average_odds VARCHAR(30) ENCODE RAW,
    average_odds_prob FLOAT ENCODE RAW,
    mid_tier_prize FLOAT ENCODE RAW,
    top_tier_prize FLOAT ENCODE RAW,
    small_prize FLOAT ENCODE RAW,
    winning_chance FLOAT ENCODE RAW,
    winning_jackpot FLOAT ENCODE RAW,
    source_system VARCHAR(30) ENCODE RAW,
    source_entity VARCHAR(30) ENCODE ZSTD,
    insert_dt DATE ENCODE RAW,
    update_dt DATE ENCODE RAW
);

-- load same data
INSERT INTO dilab_student43.dim_game_numbers_analyzedcomp
SELECT * FROM dilab_student43.dim_game_numbers;

-- Analyze compression
ANALYZE COMPRESSION dilab_student43.dim_game_numbers_analyzedcomp;





-- ======================================================================
-- Task 4
-- ======================================================================

-- a) Prepare a stored procedure.

-- disable Result Cache (to measure real query execution)
SET enable_result_cache_for_session TO OFF;


-- stored procedure: Report on original tables (2021-2022)
CREATE OR REPLACE PROCEDURE dilab_student43.report_complex_sales_2021_2022()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Drop temp table if it exists
    DROP TABLE IF EXISTS temp_complex_sales_report;

    -- Create temp table with complex report
    CREATE TEMP TABLE temp_complex_sales_report AS
    WITH sales_with_year AS (
        SELECT f.*, EXTRACT(YEAR FROM f.event_dt) AS sale_year
        FROM dilab_student43.fct_sales f
    )
    SELECT
        s.sale_year,
        g.game_category_name,
        g.game_type_name,
        r.state_name AS retailer_state,
        c.city_name AS customer_city,
        e.employee_department_name,
        pm.payment_method_name,
        COUNT(s.tickets_bought) AS total_tickets,
        SUM(s.sales) AS total_sales,
        SUM(s.payout) AS total_payout,
        SUM(s.sales) - SUM(s.payout) AS net_revenue,
        AVG(s.ticket_price) AS avg_ticket_price,
        MAX(s.sales) AS max_sale,
        MIN(s.sales) AS min_sale,
        RANK() OVER (
            PARTITION BY g.game_category_name, s.sale_year
            ORDER BY SUM(s.sales) DESC
        ) AS rank_by_game_category
    FROM sales_with_year s
    JOIN dilab_student43.dim_game_numbers g
        ON s.game_number_surr_id = g.game_number_surr_id
    JOIN dilab_student43.dim_customers_scd c
        ON s.customer_surr_id = c.customer_surr_id
    JOIN dilab_student43.dim_employees e
        ON s.employee_surr_id = e.employee_surr_id
    JOIN dilab_student43.dim_retailer_license_numbers r
        ON s.retailer_license_number_surr_id = r.retailer_license_number_surr_id
    JOIN dilab_student43.dim_payment_methods pm
        ON s.payment_method_surr_id = pm.payment_method_surr_id
    WHERE s.sale_year IN (2021, 2022)
      AND c.is_active = TRUE
      AND s.sales > 0
    GROUP BY
        s.sale_year,
        g.game_category_name,
        g.game_type_name,
        r.state_name,
        c.city_name,
        e.employee_department_name,
        pm.payment_method_name
    HAVING SUM(s.sales) > 1000
    ORDER BY sale_year, net_revenue DESC;
END;
$$;


--  Run baseline procedure and check sample results
CALL dilab_student43.report_complex_sales_2021_2022();

SELECT * FROM temp_complex_sales_report LIMIT 50;


-- b) Baseline EXPLAIN plan for analysis
EXPLAIN
WITH sales_with_year AS (
    SELECT f.*, EXTRACT(YEAR FROM f.event_dt) AS sale_year
    FROM dilab_student43.fct_sales f
)
SELECT
    s.sale_year,
    g.game_category_name,
    g.game_type_name,
    r.state_name,
    c.city_name,
    e.employee_department_name,
    pm.payment_method_name,
    COUNT(s.tickets_bought),
    SUM(s.sales),
    SUM(s.payout),
    SUM(s.sales) - SUM(s.payout),
    AVG(s.ticket_price),
    MAX(s.sales),
    MIN(s.sales),
    RANK() OVER (
        PARTITION BY g.game_category_name, s.sale_year
        ORDER BY SUM(s.sales) DESC
    )
FROM sales_with_year s
JOIN dilab_student43.dim_game_numbers g
    ON s.game_number_surr_id = g.game_number_surr_id
JOIN dilab_student43.dim_customers_scd c
    ON s.customer_surr_id = c.customer_surr_id
JOIN dilab_student43.dim_employees e
    ON s.employee_surr_id = e.employee_surr_id
JOIN dilab_student43.dim_retailer_license_numbers r
    ON s.retailer_license_number_surr_id = r.retailer_license_number_surr_id
JOIN dilab_student43.dim_payment_methods pm
    ON s.payment_method_surr_id = pm.payment_method_surr_id
WHERE s.sale_year IN (2021, 2022)
  AND c.is_active = TRUE
  AND s.sales > 0
GROUP BY
    s.sale_year, g.game_category_name, g.game_type_name,
    r.state_name, c.city_name,
    e.employee_department_name, pm.payment_method_name
HAVING SUM(s.sales) > 1000
ORDER BY sale_year, net_revenue DESC;


-- d)Inspect table metadata (distribution, size, sortkey)
SELECT 
    "schema",
    "table",
    size AS size_mb,
    diststyle,
    sortkey1 AS sort_key
FROM svv_table_info
WHERE "schema" = 'dilab_student43'
  AND "table" IN (
      'fct_sales',
      'dim_game_numbers',
      'dim_customers_scd',
      'dim_employees',
      'dim_payment_methods',
      'dim_retailer_license_numbers'
  )
ORDER BY size_mb DESC;



-- ======================================================================
-- Task 5:Let’s assume that these joins will be used very often and will not be massively shared with other tables in Redshift. Now you need to optimize your distribution style and sort keys.
--You can find more information about proper Sort/DIST key for table creation here: https://docs.aws.amazon.com/redshift/latest/dg/t_Creating_tables.html
-- ======================================================================


-- Create optimized tables with DISTKEY/SORTKEY
CREATE TABLE dilab_student43.fct_sales_opt (
    game_number_surr_id BIGINT REFERENCES dilab_student43.dim_game_numbers(game_number_surr_id),
    customer_surr_id BIGINT REFERENCES dilab_student43.dim_customers_scd(customer_surr_id),
    employee_surr_id BIGINT REFERENCES dilab_student43.dim_employees(employee_surr_id),
    retailer_license_number_surr_id BIGINT REFERENCES dilab_student43.dim_retailer_license_numbers(retailer_license_number_surr_id),
    payment_method_surr_id BIGINT REFERENCES dilab_student43.dim_payment_methods(payment_method_surr_id),
    event_dt DATE NOT NULL,
    ticket_price FLOAT,
    tickets_bought INT,
    payout FLOAT,
    sales FLOAT,
    insert_dt DATE,
    update_dt DATE,
    CONSTRAINT fct_sales_grain_unique_opt UNIQUE (
        game_number_surr_id,
        customer_surr_id,
        employee_surr_id,
        retailer_license_number_surr_id,
        payment_method_surr_id,
        event_dt
    )
)
DISTKEY(customer_surr_id)
SORTKEY(event_dt);

CREATE TABLE dilab_student43.dim_customers_scd_opt (
    customer_surr_id BIGINT PRIMARY KEY,
    customer_src_id VARCHAR(255),
    customer_name VARCHAR(100),
    customer_registration_dt DATE,
    zip_id BIGINT,
    zip_name VARCHAR(100),
    city_id BIGINT,
    city_name VARCHAR(100),
    state_id BIGINT,
    state_name VARCHAR(100),
    customer_gender VARCHAR(20),
    customer_dob DATE,
    customer_email VARCHAR(100),
    customer_phone VARCHAR(50),
    source_system VARCHAR(30),
    source_entity VARCHAR(30),
    insert_dt DATE,
    start_dt DATE,
    end_dt DATE,
    is_active BOOLEAN,
    CONSTRAINT uq_customer_src_start_opt UNIQUE (customer_src_id, start_dt)
)
DISTKEY(customer_surr_id)
SORTKEY(customer_surr_id);

CREATE TABLE dilab_student43.dim_game_numbers_opt (
    game_number_surr_id BIGINT PRIMARY KEY,
    game_number_src_id VARCHAR(255) UNIQUE,
    game_number_name VARCHAR(100),
    game_category_id BIGINT,
    game_category_name VARCHAR(100),
    game_type_id BIGINT,
    game_type_name VARCHAR(100),
    draw_dt DATE,
    average_odds VARCHAR(30),
    average_odds_prob FLOAT,
    mid_tier_prize FLOAT,
    top_tier_prize FLOAT,
    small_prize FLOAT,
    winning_chance FLOAT,
    winning_jackpot FLOAT,
    source_system VARCHAR(30),
    source_entity VARCHAR(30),
    insert_dt DATE,
    update_dt DATE
)
DISTSTYLE ALL
SORTKEY(game_number_surr_id);


--  Load optimized tables with COPY from S3
CREATE OR REPLACE PROCEDURE dilab_student43.load_optimized_tables()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Loading dim_game_numbers_opt...';
    COPY dilab_student43.dim_game_numbers_opt
    FROM 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/dim_game_numbers/dim_game_numbers.csv'
    IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
    CSV IGNOREHEADER 1 DELIMITER ',';

    RAISE NOTICE 'Loading dim_customers_scd_opt...';
    COPY dilab_student43.dim_customers_scd_opt
    FROM 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/dim_customers_scd/dim_customers_scd.csv'
    IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
    CSV IGNOREHEADER 1 DELIMITER ',';

    RAISE NOTICE 'Loading fct_sales_opt...';
    COPY dilab_student43.fct_sales_opt
    FROM 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales/fct_sales.csv'
    IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
    CSV IGNOREHEADER 1 DELIMITER ',';

    RAISE NOTICE 'All optimized tables loaded successfully.';
END;
$$;

CALL dilab_student43.load_optimized_tables();


-- Stored Procedure: Report on optimized tables

CREATE OR REPLACE PROCEDURE dilab_student43.report_complex_sales_2021_2022_opt()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS temp_complex_sales_report_opt;

    CREATE TEMP TABLE temp_complex_sales_report_opt AS
    WITH sales_with_year AS (
        SELECT f.*, EXTRACT(YEAR FROM f.event_dt) AS sale_year
        FROM dilab_student43.fct_sales_opt f
    )
    SELECT
        s.sale_year,
        g.game_category_name,
        g.game_type_name,
        r.state_name AS retailer_state,
        c.city_name AS customer_city,
        e.employee_department_name,
        pm.payment_method_name,
        COUNT(s.tickets_bought) AS total_tickets,
        SUM(s.sales) AS total_sales,
        SUM(s.payout) AS total_payout,
        SUM(s.sales) - SUM(s.payout) AS net_revenue,
        AVG(s.ticket_price) AS avg_ticket_price,
        MAX(s.sales) AS max_sale,
        MIN(s.sales) AS min_sale,
        RANK() OVER (
            PARTITION BY g.game_category_name, s.sale_year
            ORDER BY SUM(s.sales) DESC
        ) AS rank_by_game_category
    FROM sales_with_year s
    JOIN dilab_student43.dim_game_numbers_opt g
        ON s.game_number_surr_id = g.game_number_surr_id
    JOIN dilab_student43.dim_customers_scd_opt c
        ON s.customer_surr_id = c.customer_surr_id
    JOIN dilab_student43.dim_employees e
        ON s.employee_surr_id = e.employee_surr_id
    JOIN dilab_student43.dim_retailer_license_numbers r
        ON s.retailer_license_number_surr_id = r.retailer_license_number_surr_id
    JOIN dilab_student43.dim_payment_methods pm
        ON s.payment_method_surr_id = pm.payment_method_surr_id
    WHERE s.sale_year IN (2021, 2022)
      AND c.is_active = TRUE
      AND s.sales > 0
    GROUP BY
        s.sale_year,
        g.game_category_name,
        g.game_type_name,
        r.state_name,
        c.city_name,
        e.employee_department_name,
        pm.payment_method_name
    HAVING SUM(s.sales) > 1000
    ORDER BY sale_year, net_revenue DESC;
END;
$$;

CALL dilab_student43.report_complex_sales_2021_2022_opt();


-- Run ANALYZE and count for sanity checks

ANALYZE dilab_student43.dim_game_numbers_opt;

SELECT COUNT(*) FROM dilab_student43.fct_sales_opt;


-- EXPLAIN plan for optimized version

EXPLAIN
WITH sales_with_year AS (
    SELECT f.*, EXTRACT(YEAR FROM f.event_dt) AS sale_year
    FROM dilab_student43.fct_sales_opt f
)
SELECT
    s.sale_year,
    g.game_category_name,
    g.game_type_name,
    r.state_name,
    c.city_name,
    e.employee_department_name,
    pm.payment_method_name,
    COUNT(s.tickets_bought),
    SUM(s.sales),
    SUM(s.payout),
    SUM(s.sales) - SUM(s.payout),
    AVG(s.ticket_price),
    MAX(s.sales),
    MIN(s.sales),
    RANK() OVER (
        PARTITION BY g.game_category_name, s.sale_year
        ORDER BY SUM(s.sales) DESC
    )
FROM sales_with_year s
JOIN dilab_student43.dim_game_numbers_opt g
    ON s.game_number_surr_id = g.game_number_surr_id
JOIN dilab_student43.dim_customers_scd_opt c
    ON s.customer_surr_id = c.customer_surr_id
JOIN dilab_student43.dim_employees e
    ON s.employee_surr_id = e.employee_surr_id
JOIN dilab_student43.dim_retailer_license_numbers r
    ON s.retailer_license_number_surr_id = r.retailer_license_number_surr_id
JOIN dilab_student43.dim_payment_methods pm
    ON s.payment_method_surr_id = pm.payment_method_surr_id
WHERE s.sale_year IN (2021, 2022)
  AND c.is_active = TRUE
  AND s.sales > 0
GROUP BY
    s.sale_year, g.game_category_name, g.game_type_name,
    r.state_name, c.city_name,
    e.employee_department_name, pm.payment_method_name
HAVING SUM(s.sales) > 1000
ORDER BY sale_year, net_revenue DESC;

-- ======================================================================
-- Task 6: Run the stored procedure using optimized tables and load data to your report.
-- ======================================================================

-- Call the optimized stored procedure
CALL dilab_student43.report_complex_sales_2021_2022_opt();

-- Now the results are in temp_complex_sales_report_opt.
-- Copy them into a permanent table/report table.
DROP TABLE IF EXISTS dilab_student43.report_complex_sales_2021_2022_final;

CREATE TABLE dilab_student43.report_complex_sales_2021_2022_final AS
SELECT * 
FROM temp_complex_sales_report_opt;

-- Verify the report table
SELECT COUNT(*) AS rows_loaded
FROM dilab_student43.report_complex_sales_2021_2022_final;

-- Optionally preview some rows
SELECT * 
FROM dilab_student43.report_complex_sales_2021_2022_final
LIMIT 20;




-- ======================================================================
-- WORKING WITH EXTERNAL TABLES
-- ======================================================================

-- ======================================================================
-- Task 1:Create external schema (user_dilab_student(1..32)_ext) pointing to your location and create several external tables on your files. More info can be 
--found here:
-- https://docs.amazonaws.cn/en_us/redshift/latest/dg/c-getting-started-using-spectrum- create-external-table.html
-- https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html
-- ======================================================================


-- This points Redshift Spectrum to your Glue catalog and S3
CREATE EXTERNAL SCHEMA IF NOT EXISTS dilab_student43_ext
FROM DATA CATALOG
DATABASE 'gabriela-cretu-bl-dm'  -- your Glue database
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role';


-- ========================================
-- Task 2: Partitioned external tables are extremely useful for the performance and cost cuts.
-- ========================================

--a)Export any data which contain date column into S3 in a way, so each subfolder contains 1 month data (e.g., subfolder /your_date_column=2018-03-01 
--contains all records of the table where your_date_column is within March 2018).

CREATE OR REPLACE PROCEDURE dilab_student43.unload_sales_month(
    p_from_date TEXT,
    p_to_date   TEXT,
    p_s3_path   TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE
        'UNLOAD (''SELECT * FROM dilab_student43.fct_sales ' ||
        'WHERE event_dt >= DATE ''''' || p_from_date || ''''' ' ||
        'AND event_dt < DATE ''''' || p_to_date || ''''' '') ' ||
        'TO ''' || p_s3_path || ''' ' ||
        'IAM_ROLE ''arn:aws:iam::260586643565:role/dilab-redshift-role'' ' ||
        'ALLOWOVERWRITE ' ||      -- optional, safe for reruns
        'FORMAT AS CSV ' ||
        'PARALLEL OFF;';
END;
$$;


CREATE OR REPLACE PROCEDURE dilab_student43.unload_sales_all_months()
LANGUAGE plpgsql
AS $$
DECLARE
    start_month DATE := '2021-01-01';
    end_month DATE := '2022-12-01';
    current_month DATE := start_month;
    next_month DATE;
    s3_path TEXT;
BEGIN
    WHILE current_month <= end_month LOOP
        next_month := current_month + INTERVAL '1 month';
        s3_path := 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=' 
                    || TO_CHAR(current_month, 'YYYY-MM-DD') || '/';
        
        -- Call single-month unload procedure
        CALL dilab_student43.unload_sales_month(
            TO_CHAR(current_month, 'YYYY-MM-DD'),
            TO_CHAR(next_month, 'YYYY-MM-DD'),
            s3_path
        );
        
        current_month := next_month;
    END LOOP;

    RAISE NOTICE '✅ All months processed.';
END;
$$;

CALL dilab_student43.unload_sales_all_months();

--b)Create PARTITIONED external table “ext_studentN_partitioned” (partition by your_date_column)..

drop table dilab_student43_ext.ext_fct_sales;
DROP TABLE IF EXISTS dilab_student43_ext.ext_fct_sales;

CREATE EXTERNAL TABLE dilab_student43_ext.ext_fct_sales (
    game_number_surr_id BIGINT,
    customer_surr_id BIGINT,
    employee_surr_id BIGINT,
    retailer_license_number_surr_id BIGINT,
    payment_method_surr_id BIGINT,
    event_dt DATE,
    ticket_price FLOAT,
    tickets_bought INT,
    payout FLOAT,
    sales FLOAT,
    insert_dt DATE,
    update_dt DATE
)
PARTITIONED BY (event_month VARCHAR)  -- ✅ use VARCHAR for partition
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/';

-- Generate ALTER TABLE statements for each month
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-01-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-01-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-02-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-02-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-03-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-03-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-04-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-04-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-05-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-05-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-06-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-06-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-07-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-07-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-08-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-08-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-09-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-09-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-10-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-10-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-11-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-11-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2021-12-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2021-12-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-01-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-01-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-02-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-02-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-03-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-03-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-04-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-04-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-05-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-05-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-06-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-06-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-07-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-07-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-08-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-08-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-09-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-09-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-10-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-10-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-11-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-11-01/';
ALTER TABLE dilab_student43_ext.ext_fct_sales ADD IF NOT EXISTS PARTITION (event_month='2022-12-01') LOCATION 's3://gabriela-cretu-bl-dm-bucket/di_dwh/bl_dm/fct_sales_partitioned/event_month=2022-12-01/';



--c)Verify data in partitioned external table (compare number of records per month to original table from which you prepared files or just with rows in the files). 
--Prepare the test script that will show 0 difference (if it is not 0 – there is an issue).

WITH original AS (
    SELECT DATE_TRUNC('month', event_dt) AS month, COUNT(*) AS original_count
    FROM dilab_student43.fct_sales
    GROUP BY 1
),
external AS (
    SELECT CAST(event_month AS DATE) AS month, COUNT(*) AS external_count
    FROM dilab_student43_ext.ext_fct_sales
    GROUP BY 1
)
SELECT 
    o.month,
    COALESCE(o.original_count,0) AS original_count,
    COALESCE(e.external_count,0) AS external_count,
    COALESCE(o.original_count,0) - COALESCE(e.external_count,0) AS difference
FROM original o
FULL OUTER JOIN external e ON o.month = e.month
ORDER BY o.month;

--d)Examine query plan where you select from ext_studentN_partitioned with a WHERE clause containing your_date_column condition. Describe it.

SELECT *
FROM dilab_student43_ext.ext_fct_sales
WHERE event_month = '2021-03-01';

EXPLAIN
SELECT *
FROM dilab_student43_ext.ext_fct_sales
WHERE event_month = '2021-03-01';