-- ======================================
-- Optional:Delete the content of the tables
-- ======================================
DELETE FROM bl_3nf.ce_cities;
DELETE FROM bl_3nf.ce_customers_scd;
DELETE FROM bl_3nf.ce_departments;
DELETE FROM bl_3nf.ce_employees;
DELETE FROM bl_3nf.ce_game_categories;
DELETE FROM bl_3nf.ce_game_numbers;
DELETE FROM bl_3nf.ce_game_types;
DELETE FROM bl_3nf.ce_location_names;
DELETE FROM bl_3nf.ce_payment_methods;
DELETE FROM bl_3nf.ce_retailer_license_numbers;
DELETE FROM bl_3nf.ce_sales;
DELETE FROM bl_3nf.ce_states;
DELETE FROM bl_3nf.ce_statuses;
DELETE FROM bl_3nf.ce_zip;

-- ======================================
-- Drop sequences for dimension tables if they exist
-- ======================================
DROP SEQUENCE IF EXISTS CE_GAME_TYPES_SEQ;
DROP SEQUENCE IF EXISTS CE_GAME_CATEGORIES_SEQ;
DROP SEQUENCE IF EXISTS CE_GAME_NUMBERS_SEQ;
DROP SEQUENCE IF EXISTS CE_PAYMENT_METHODS_SEQ;
DROP SEQUENCE IF EXISTS CE_STATES_SEQ;
DROP SEQUENCE IF EXISTS CE_CITIES_SEQ;
DROP SEQUENCE IF EXISTS CE_ZIP_SEQ;
DROP SEQUENCE IF EXISTS CE_EMPLOYEES_SEQ;
DROP SEQUENCE IF EXISTS CE_DEPARTMENTS_SEQ;
DROP SEQUENCE IF EXISTS CE_STATUSES_SEQ;
DROP SEQUENCE IF EXISTS CE_RETAILERS_SEQ;
DROP SEQUENCE IF EXISTS CE_LOCATIONS_SEQ;
DROP SEQUENCE IF EXISTS CE_CUSTOMERS_SEQ;

-- ======================================
-- Create sequences for dimension tables
-- ======================================
CREATE SEQUENCE IF NOT EXISTS CE_GAME_TYPES_SEQ     START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_GAME_CATEGORIES_SEQ START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_GAME_NUMBERS_SEQ    START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_PAYMENT_METHODS_SEQ START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_STATES_SEQ          START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_CITIES_SEQ          START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_ZIP_SEQ             START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_EMPLOYEES_SEQ       START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_DEPARTMENTS_SEQ     START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_STATUSES_SEQ        START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_RETAILERS_SEQ       START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_LOCATIONS_SEQ       START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS CE_CUSTOMERS_SEQ       START 1 INCREMENT 1;

-- ======================================
-- Game Types
-- ======================================


-- bl_3nf.ce_game_types


WITH scratch AS (
    SELECT DISTINCT
        game_type AS game_type_src_id,
        'SCRATCH' AS source_system,
        'SRC_FINAL_SCRATCH' AS source_entity
    FROM sa_final_scratch.src_final_scratch
    WHERE game_type IS NOT NULL
),
draw AS (
    SELECT DISTINCT
        game_type AS game_type_src_id,
        'DRAW' AS source_system,
        'SRC_FINAL_DRAW' AS source_entity
    FROM sa_final_draw.src_final_draw
    WHERE game_type IS NOT NULL
),
merged AS (
    SELECT
        COALESCE(s.game_type_src_id, d.game_type_src_id, 'n. a.') AS game_type_src_id,
        CASE
            WHEN s.game_type_src_id IS NOT NULL AND d.game_type_src_id IS NOT NULL THEN 'SCRATCH,DRAW'
            WHEN s.game_type_src_id IS NOT NULL THEN 'sa_final_scratch'
            WHEN d.game_type_src_id IS NOT NULL THEN 'sa_final_draw'
            ELSE 'MANUAL'
        END AS source_system,
        CASE
            WHEN s.game_type_src_id IS NOT NULL AND d.game_type_src_id IS NOT NULL THEN 'SRC_FINAL_SCRATCH,SRC_FINAL_DRAW'
            WHEN s.game_type_src_id IS NOT NULL THEN 'src_final_scratch'
            WHEN d.game_type_src_id IS NOT NULL THEN 'src_final_draw'
            ELSE 'MANUAL'
        END AS source_entity,
        CURRENT_DATE AS insert_dt,
        CURRENT_DATE AS update_dt
    FROM scratch s
    FULL OUTER JOIN draw d ON s.game_type_src_id = d.game_type_src_id
)
INSERT INTO bl_3nf.ce_game_types (
    game_type_id,
    game_type_src_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_game_types_seq'),
    game_type_src_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
FROM merged;


-- ======================================
-- Game Categories
-- ======================================

WITH new_categories AS (
    SELECT DISTINCT ON (COALESCE(s.game_category::VARCHAR, d.game_category::VARCHAR))
        COALESCE(s.game_category::VARCHAR, d.game_category::VARCHAR, 'n. a.') AS game_category_src_id,
        COALESCE(gt.game_type_id, -1) AS game_type_id,
        COALESCE(CAST(d.winning_chance AS FLOAT), -1) AS winning_chance,
        COALESCE(CAST(d.winning_jackpot AS FLOAT), -1) AS winning_jackpot,
        CASE
            WHEN s.game_category IS NOT NULL AND d.game_category IS NOT NULL THEN 'sa_final_scratch,sa_final_draw'
            WHEN s.game_category IS NOT NULL THEN 'sa_final_scratch'
            WHEN d.game_category IS NOT NULL THEN 'sa_final_draw'
            ELSE 'n. a.'
        END AS source_system,
        CASE
            WHEN s.game_category IS NOT NULL AND d.game_category IS NOT NULL THEN 'src_final_scratch,src_final_draw'
            WHEN s.game_category IS NOT NULL THEN 'src_final_scratch'
            WHEN d.game_category IS NOT NULL THEN 'src_final_draw'
            ELSE 'n. a.'
        END AS source_entity
    FROM
        sa_final_scratch.src_final_scratch s
    FULL OUTER JOIN
        sa_final_draw.src_final_draw d ON s.game_category = d.game_category
    LEFT JOIN
        bl_3nf.ce_game_types gt ON COALESCE(s.game_type, d.game_type) = gt.game_type_src_id
    WHERE
        COALESCE(s.game_category::VARCHAR, d.game_category::VARCHAR) IS NOT NULL
        AND COALESCE(s.game_category::VARCHAR, d.game_category::VARCHAR) NOT IN (
            SELECT game_category_src_id FROM bl_3nf.ce_game_categories
        )
)

INSERT INTO bl_3nf.ce_game_categories (
    game_category_id,
    game_category_src_id,
    game_type_id,
    winning_chance,
    winning_jackpot,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_game_categories_seq'),
    game_category_src_id,
    game_type_id,
    winning_chance,
    winning_jackpot,
    source_system,
    source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM new_categories;


-- ======================================
-- Game Numbers
-- ======================================

WITH new_game_numbers AS (
    SELECT DISTINCT ON (COALESCE(s.game_number::VARCHAR, d.game_number::VARCHAR))
        COALESCE(s.game_number::VARCHAR, d.game_number::VARCHAR, 'n. a.') AS game_number_src_id,
        COALESCE(c.game_category_id, -1)::BIGINT AS game_category_id,
        COALESCE(d.draw_dt_id::DATE, '1900-01-01') AS draw_dt_id,
        COALESCE(CAST(s.average_odds AS VARCHAR(30)), 'n. a.') AS average_odds,
        COALESCE(CAST(s.average_odds_prob AS FLOAT), -1) AS average_odds_prob,
        COALESCE(CAST(s.mid_prize AS FLOAT), -1) AS mid_tier_prize,
        COALESCE(CAST(s.top_prize AS FLOAT), -1) AS top_tier_prize,
        COALESCE(CAST(s.small_prize AS FLOAT), -1) AS small_prize,
        CASE
            WHEN s.game_number IS NOT NULL AND d.game_number IS NOT NULL THEN 'sa_final_scratch,sa_final_draw'
            WHEN s.game_number IS NOT NULL THEN 'sa_final_scratch'
            WHEN d.game_number IS NOT NULL THEN 'sa_final_draw'
            ELSE 'n. a.'
        END AS source_system,
        CASE
            WHEN s.game_number IS NOT NULL AND d.game_number IS NOT NULL THEN 'src_final_scratch,src_final_draw'
            WHEN s.game_number IS NOT NULL THEN 'src_final_scratch'
            WHEN d.game_number IS NOT NULL THEN 'src_final_draw'
            ELSE 'n. a.'
        END AS source_entity
    FROM
        sa_final_scratch.src_final_scratch s
    FULL OUTER JOIN
        sa_final_draw.src_final_draw d ON s.game_number = d.game_number
    LEFT JOIN
        bl_3nf.ce_game_categories c ON COALESCE(s.game_category, d.game_category) = c.game_category_src_id
    WHERE
        COALESCE(s.game_number::VARCHAR, d.game_number::VARCHAR) IS NOT NULL
        AND COALESCE(s.game_number::VARCHAR, d.game_number::VARCHAR) NOT IN (
            SELECT game_number_src_id FROM bl_3nf.ce_game_numbers
        )
)

INSERT INTO bl_3nf.ce_game_numbers (
    game_number_id,
    game_number_src_id,
    game_category_id,
    draw_dt_id,
    average_odds,
    average_odds_prob,
    mid_tier_prize,
    top_tier_prize,
    small_prize,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_game_numbers_seq'),
    game_number_src_id,
    game_category_id,
    draw_dt_id,
    average_odds,
    average_odds_prob,
    mid_tier_prize,
    top_tier_prize,
    small_prize,
    source_system,
    source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM new_game_numbers;


-- ======================================
-- Payment Methods
-- ======================================

WITH all_payment_methods AS (
    SELECT 
        COALESCE(payment_method_id::VARCHAR, 'n. a.') AS payment_method_src_id,
        COALESCE(payment_method_name::VARCHAR, 'n. a.') AS payment_method_name,
        'sa_final_scratch' AS source_system,
        'src_final_scratch' AS source_entity
    FROM sa_final_scratch.src_final_scratch

    UNION ALL

    SELECT 
        COALESCE(payment_method_id::VARCHAR, 'n. a.') AS payment_method_src_id,
        COALESCE(payment_method_name::VARCHAR, 'n. a.') AS payment_method_name,
        'sa_final_draw' AS source_system,
        'src_final_draw' AS source_entity
    FROM sa_final_draw.src_final_draw
),
distinct_payment_methods AS (
    SELECT DISTINCT ON (payment_method_src_id) *
    FROM all_payment_methods
    WHERE payment_method_src_id NOT IN (
        SELECT payment_method_src_id FROM bl_3nf.ce_payment_methods
    )
)

INSERT INTO bl_3nf.ce_payment_methods (
    payment_method_id,
    payment_method_src_id,
    payment_method_name,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_payment_methods_seq'),
    payment_method_src_id,
    payment_method_name,
    source_system,
    source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM distinct_payment_methods;



-- ======================================
-- States
-- ======================================

WITH all_states AS (
    SELECT 
        COALESCE(retailer_location_state::VARCHAR, 'n. a.') AS state_src_id,
        'sa_final_draw' AS source_system,
        'src_final_draw' AS source_entity
    FROM sa_final_draw.src_final_draw

    UNION

    SELECT 
        COALESCE(customer_state::VARCHAR, 'n. a.') AS state_src_id,
        'sa_final_draw' AS source_system,
        'src_final_draw' AS source_entity
    FROM sa_final_draw.src_final_draw
),
distinct_states AS (
    SELECT DISTINCT ON (state_src_id) *
    FROM all_states
    WHERE state_src_id NOT IN (
        SELECT state_src_id FROM bl_3nf.ce_states
    )
)

INSERT INTO bl_3nf.ce_states (
    state_id,
    state_src_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_states_seq'),
    state_src_id,
    source_system,
    source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM distinct_states;



-- ======================================
-- Cities
-- ======================================

WITH
-- Customer cities from draw
draw_customer_cities AS (
    SELECT DISTINCT
        customer_id,
        COALESCE(customer_city::VARCHAR, 'n. a.') AS city_src_id
    FROM sa_final_draw.src_final_draw
),

-- Retailer cities from scratch
scratch_retailer_cities AS (
    SELECT DISTINCT
        retailer_license_number,
        COALESCE(retailer_location_city::VARCHAR, 'n. a.') AS city_src_id
    FROM sa_final_scratch.src_final_scratch
),

-- Customer states from draw
draw_customer_states AS (
    SELECT DISTINCT
        customer_id,
        COALESCE(customer_state::VARCHAR, 'n. a.') AS state_src_id
    FROM sa_final_draw.src_final_draw
),

-- Retailer states from draw
draw_retailer_states AS (
    SELECT DISTINCT
        retailer_license_number,
        COALESCE(retailer_location_state::VARCHAR, 'n. a.') AS state_src_id
    FROM sa_final_draw.src_final_draw
),

-- Join customer cities to states
customer_city_state AS (
    SELECT
        c.city_src_id,
        COALESCE(s.state_src_id, 'n. a.') AS state_src_id
    FROM draw_customer_cities c
    LEFT JOIN draw_customer_states s ON c.customer_id = s.customer_id
),

-- Join retailer cities to states
retailer_city_state AS (
    SELECT
        r.city_src_id,
        COALESCE(s.state_src_id, 'n. a.') AS state_src_id
    FROM scratch_retailer_cities r
    LEFT JOIN draw_retailer_states s ON r.retailer_license_number = s.retailer_license_number
),

-- Combine both
combined_city_state AS (
    SELECT * FROM customer_city_state
    UNION ALL
    SELECT * FROM retailer_city_state
),

-- Map to state_id and exclude existing cities
mapped_cities AS (
    SELECT DISTINCT ON (city_src_id)
        ccs.city_src_id,
        COALESCE(st.state_id, -1) AS state_id
    FROM combined_city_state ccs
    LEFT JOIN bl_3nf.ce_states st ON ccs.state_src_id = st.state_src_id
    WHERE ccs.city_src_id NOT IN (
        SELECT city_src_id FROM bl_3nf.ce_cities
    )
)

INSERT INTO bl_3nf.ce_cities (
    city_id,
    city_src_id,
    state_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_cities_seq'),
    city_src_id,
    state_id,
    CASE
        WHEN city_src_id IN (SELECT city_src_id FROM draw_customer_cities) THEN 'sa_final_draw'
        ELSE 'sa_final_scratch'
    END AS source_system,
    CASE
        WHEN city_src_id IN (SELECT city_src_id FROM draw_customer_cities) THEN 'src_final_draw'
        ELSE 'src_final_scratch'
    END AS source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM mapped_cities;




-- ======================================
-- Zipcodes
-- ======================================

WITH
-- Customer ZIPs from draw (have both zip and city)
draw_customer_zip_city AS (
    SELECT DISTINCT
        COALESCE(customer_zip_code::VARCHAR, 'n. a.') AS zip_src_id,
        COALESCE(customer_city::VARCHAR, 'n. a.') AS city_src_id,
        'sa_final_draw' AS source_system,
        'src_final_draw' AS source_entity
    FROM sa_final_draw.src_final_draw
),

-- Retailer ZIPs from scratch (have both zip and city)
scratch_retailer_zip_city AS (
    SELECT DISTINCT
        COALESCE(retailer_location_zip_code::VARCHAR, 'n. a.') AS zip_src_id,
        COALESCE(retailer_location_city::VARCHAR, 'n. a.') AS city_src_id,
        'sa_final_scratch' AS source_system,
        'src_final_scratch' AS source_entity
    FROM sa_final_scratch.src_final_scratch
),

-- Retailer ZIPs from draw (no city info available)
draw_retailer_zip_no_city AS (
    SELECT DISTINCT
        COALESCE(retailer_location_zip_code::VARCHAR, 'n. a.') AS zip_src_id,
        -1 AS city_id,
        'sa_final_draw' AS source_system,
        'src_final_draw' AS source_entity
    FROM sa_final_draw.src_final_draw
    WHERE retailer_location_zip_code IS NOT NULL
),

-- Combined ZIP-city records where city is known
zip_city_with_match AS (
    SELECT zip_src_id, city_src_id, source_system, source_entity
    FROM draw_customer_zip_city
    UNION ALL
    SELECT zip_src_id, city_src_id, source_system, source_entity
    FROM scratch_retailer_zip_city
),

-- Map city_src_id to city_id
mapped_zip_city AS (
    SELECT DISTINCT ON (z.zip_src_id)
        z.zip_src_id,
        COALESCE(c.city_id, -1) AS city_id,
        z.source_system,
        z.source_entity
    FROM zip_city_with_match z
    LEFT JOIN bl_3nf.ce_cities c ON z.city_src_id = c.city_src_id
    WHERE z.zip_src_id NOT IN (
        SELECT zip_src_id FROM bl_3nf.ce_zip
    )
),

-- Add draw-only zips with no city, fallback to city_id -1
draw_fallback_zips AS (
    SELECT DISTINCT ON (zip_src_id)
        zip_src_id,
        city_id,
        source_system,
        source_entity
    FROM draw_retailer_zip_no_city
    WHERE zip_src_id NOT IN (
        SELECT zip_src_id FROM bl_3nf.ce_zip
        UNION
        SELECT zip_src_id FROM mapped_zip_city
    )
),

-- Combine everything into a distinct set of zips
final_zip_insert AS (
    SELECT * FROM mapped_zip_city
    UNION ALL
    SELECT * FROM draw_fallback_zips
)

INSERT INTO bl_3nf.ce_zip (
    zip_id,
    zip_src_id,
    city_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_zip_seq'),
    zip_src_id,
    city_id,
    source_system,
    source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM final_zip_insert;



-- ======================================
-- Location Names
-- ======================================

WITH
-- Retailer location names and zips from draw
draw_location_zip AS (
    SELECT DISTINCT
        COALESCE(retailer_location_name::VARCHAR, 'n. a.') AS location_name_src_id,
        COALESCE(retailer_location_zip_code::VARCHAR, 'n. a.') AS zip_src_id,
        'sa_final_draw' AS source_system,
        'src_final_draw' AS source_entity
    FROM sa_final_draw.src_final_draw
),

-- Retailer location names and zips from scratch
scratch_location_zip AS (
    SELECT DISTINCT
        COALESCE(retailer_location_name::VARCHAR, 'n. a.') AS location_name_src_id,
        COALESCE(retailer_location_zip_code::VARCHAR, 'n. a.') AS zip_src_id,
        'sa_final_scratch' AS source_system,
        'src_final_scratch' AS source_entity
    FROM sa_final_scratch.src_final_scratch
),

-- Combine all sources
all_location_zip AS (
    SELECT * FROM draw_location_zip
    UNION ALL
    SELECT * FROM scratch_location_zip
),

-- Map zip_src_id to zip_id
mapped_locations AS (
    SELECT DISTINCT ON (lz.location_name_src_id)
        lz.location_name_src_id,
        COALESCE(z.zip_id, -1) AS zip_id,
        lz.source_system,
        lz.source_entity
    FROM all_location_zip lz
    LEFT JOIN bl_3nf.ce_zip z ON lz.zip_src_id = z.zip_src_id
    WHERE lz.location_name_src_id NOT IN (
        SELECT location_name_src_id FROM bl_3nf.ce_location_names
    )
)

INSERT INTO bl_3nf.ce_location_names (
    location_name_id,
    location_name_src_id,
    zip_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_locations_seq'),
    location_name_src_id,
    zip_id,
    source_system,
    source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM mapped_locations;

-- ======================================
-- Retailers
-- ======================================

WITH
-- Retailer license + location name from draw
draw_retailers AS (
    SELECT DISTINCT
        COALESCE(retailer_license_number::VARCHAR, 'n. a.') AS license_src_id,
        COALESCE(retailer_location_name::VARCHAR, 'n. a.') AS location_name_src_id,
        'sa_final_draw' AS source_system,
        'src_final_draw' AS source_entity
    FROM sa_final_draw.src_final_draw
),

-- Retailer license + location name from scratch
scratch_retailers AS (
    SELECT DISTINCT
        COALESCE(retailer_license_number::VARCHAR, 'n. a.') AS license_src_id,
        COALESCE(retailer_location_name::VARCHAR, 'n. a.') AS location_name_src_id,
        'sa_final_scratch' AS source_system,
        'src_final_scratch' AS source_entity
    FROM sa_final_scratch.src_final_scratch
),

-- Combine both sources
all_retailers AS (
    SELECT * FROM draw_retailers
    UNION ALL
    SELECT * FROM scratch_retailers
),

-- Map to LOCATION_NAME_ID
mapped_licenses AS (
    SELECT DISTINCT ON (r.license_src_id)
        r.license_src_id,
        COALESCE(l.location_name_id, -1) AS location_name_id,
        r.source_system,
        r.source_entity
    FROM all_retailers r
    LEFT JOIN bl_3nf.ce_location_names l
        ON r.location_name_src_id = l.location_name_src_id
    WHERE r.license_src_id NOT IN (
        SELECT retailer_license_number_src_id FROM bl_3nf.ce_retailer_license_numbers
    )
)

INSERT INTO bl_3nf.ce_retailer_license_numbers (
    retailer_license_number_id,
    retailer_license_number_src_id,
    retailer_location_name_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_retailers_seq'),
    license_src_id,
    location_name_id,
    source_system,
    source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM mapped_licenses;


-- ======================================
-- Statuses
-- ======================================

WITH
-- Statuses from scratch only
scratch_statuses AS (
    SELECT DISTINCT
        COALESCE(employee_status::VARCHAR, 'n. a.') AS status_src_id,
        'sa_final_scratch' AS source_system,
        'src_final_scratch' AS source_entity
    FROM sa_final_scratch.src_final_scratch
    WHERE employee_status IS NOT NULL
),

-- Filter out statuses already in target table
filtered_statuses AS (
    SELECT *
    FROM scratch_statuses
    WHERE status_src_id NOT IN (
        SELECT status_src_id FROM bl_3nf.ce_statuses
    )
)

INSERT INTO bl_3nf.ce_statuses (
    status_id,
    status_src_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_statuses_seq'),
    status_src_id,
    source_system,
    source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM filtered_statuses;


-- ======================================
-- Departments
-- ======================================

WITH
-- Departments from scratch only
scratch_departments AS (
    SELECT DISTINCT
        COALESCE(employee_department::VARCHAR, 'n. a.') AS department_src_id,
        'sa_final_scratch' AS source_system,
        'src_final_scratch' AS source_entity
    FROM sa_final_scratch.src_final_scratch
    WHERE employee_department IS NOT NULL
),

-- Filter out already inserted departments
filtered_departments AS (
    SELECT *
    FROM scratch_departments
    WHERE department_src_id NOT IN (
        SELECT department_src_id FROM bl_3nf.ce_departments
    )
)

INSERT INTO bl_3nf.ce_departments (
    department_id,
    department_src_id,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_departments_seq'),
    department_src_id,
    source_system,
    source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM filtered_departments;


-- ======================================
-- Employees
-- ======================================

WITH
-- Employees from scratch source
scratch_employees AS (
    SELECT DISTINCT
        employee_id::VARCHAR(255) AS employee_src_id,
        employee_name,
        employee_department AS department_src_id,
        employee_status AS status_src_id
    FROM sa_final_scratch.src_final_scratch
    WHERE employee_id IS NOT NULL
),

-- Employees from draw source
draw_employees AS (
    SELECT DISTINCT
        employee_id::VARCHAR(255) AS employee_src_id,
        employee_email,
        employee_phone,
        employee_hire_dt_id,
        employee_salary
    FROM sa_final_draw.src_final_draw
    WHERE employee_id IS NOT NULL
),

-- Combine both using COALESCE with casting inside
combined_employees AS (
    SELECT
        COALESCE(s.employee_src_id::VARCHAR(255), d.employee_src_id::VARCHAR(255)) AS employee_src_id,
        COALESCE(s.employee_name::VARCHAR(50), 'n. a.') AS employee_name,
        COALESCE(s.department_src_id, 'n. a.') AS department_src_id,
        COALESCE(s.status_src_id, 'n. a.') AS status_src_id,
        COALESCE(d.employee_email::VARCHAR(100), 'n. a.') AS employee_email,
        COALESCE(d.employee_phone::VARCHAR(50), 'n. a.') AS employee_phone,
        COALESCE(d.employee_hire_dt_id::INT, -1) AS employee_hire_dt_id,
        COALESCE(d.employee_salary::FLOAT, -1) AS employee_salary,
        CASE 
            WHEN s.employee_src_id IS NOT NULL THEN 'sa_final_scratch'
            ELSE 'sa_final_draw'
        END::VARCHAR(30) AS source_system,
        CASE 
            WHEN s.employee_src_id IS NOT NULL THEN 'src_final_scratch'
            ELSE 'src_final_draw'
        END::VARCHAR(30) AS source_entity
    FROM scratch_employees s
    FULL OUTER JOIN draw_employees d ON s.employee_src_id = d.employee_src_id
),

-- Map to target IDs and filter out existing ones
mapped_employees AS (
    SELECT
        ce.employee_src_id,
        ce.employee_name,
        COALESCE(dep.department_id::BIGINT, -1) AS employee_department_id,
        COALESCE(st.status_id::BIGINT, -1) AS employee_status_id,
        ce.employee_hire_dt_id,
        ce.employee_email,
        ce.employee_phone,
        ce.employee_salary,
        ce.source_system,
        ce.source_entity
    FROM combined_employees ce
    LEFT JOIN bl_3nf.ce_departments dep ON ce.department_src_id = dep.department_src_id
    LEFT JOIN bl_3nf.ce_statuses st ON ce.status_src_id = st.status_src_id
    WHERE ce.employee_src_id NOT IN (
        SELECT employee_src_id FROM bl_3nf.ce_employees
    )
)

-- Insert new employees
INSERT INTO bl_3nf.ce_employees (
    employee_id,
    employee_src_id,
    employee_department_id,
    employee_status_id,
    employee_hire_dt_id,
    employee_name,
    employee_email,
    employee_phone,
    employee_salary,
    source_system,
    source_entity,
    insert_dt,
    update_dt
)
SELECT
    NEXTVAL('ce_employees_seq') AS employee_id,
    employee_src_id,
    employee_department_id,
    employee_status_id,
    employee_hire_dt_id,
    employee_name,
    employee_email,
    employee_phone,
    employee_salary,
    source_system,
    source_entity,
    CURRENT_DATE,
    CURRENT_DATE
FROM mapped_employees;

-- Optional: View inserted employees




-- ======================================
-- Customers
-- ======================================

WITH
-- Raw data from scratch (name, gender, dob)
scratch_data AS (
    SELECT DISTINCT
        customer_id::VARCHAR AS customer_src_id,
        customer_name,
        customer_gender,
        customer_dob
    FROM sa_final_scratch.src_final_scratch
    WHERE customer_id IS NOT NULL
),

-- Raw data from draw (zip, registration date, email, phone)
draw_data AS (
    SELECT DISTINCT
        customer_id::VARCHAR AS customer_src_id,
        customer_zip_code,
        customer_registration_dt_id,
        customer_email,
        customer_phone
    FROM sa_final_draw.src_final_draw
    WHERE customer_id IS NOT NULL
),

-- Combine scratch and draw data, assign source system/entity
combined_customers AS (
    SELECT
        COALESCE(s.customer_src_id, d.customer_src_id) AS customer_src_id,
        s.customer_name,
        s.customer_gender,
        s.customer_dob,
        d.customer_zip_code,
        d.customer_registration_dt_id,
        d.customer_email,
        d.customer_phone,
        CASE 
            WHEN s.customer_src_id IS NOT NULL THEN 'sa_final_scratch'
            ELSE 'sa_final_draw'
        END AS source_system,
        CASE 
            WHEN s.customer_src_id IS NOT NULL THEN 'src_final_scratch'
            ELSE 'src_final_draw'
        END AS source_entity
    FROM scratch_data s
    FULL OUTER JOIN draw_data d ON s.customer_src_id = d.customer_src_id
),

-- Map and filter out existing customers
mapped_customers AS (
    SELECT
        cc.customer_src_id,
        COALESCE(z.zip_id, -1) AS customer_zip_code_id,
        COALESCE(cc.customer_registration_dt_id::INT, -1) AS customer_registration_dt_id,
        COALESCE(cc.customer_name, 'n.a.') AS customer_name,
        COALESCE(cc.customer_gender, 'n.a.') AS customer_gender,
        COALESCE(cc.customer_dob::DATE, DATE '1900-01-01') AS customer_dob,
        COALESCE(cc.customer_email, 'n.a.') AS customer_email,
        COALESCE(cc.customer_phone, 'n.a.') AS customer_phone,
        cc.source_system,
        cc.source_entity
    FROM combined_customers cc
    LEFT JOIN bl_3nf.ce_zip z ON cc.customer_zip_code = z.zip_src_id
    WHERE cc.customer_src_id NOT IN (
        SELECT customer_src_id FROM bl_3nf.ce_customers_scd
    )
)

INSERT INTO bl_3nf.ce_customers_scd (
    customer_id,
    customer_src_id,
    customer_zip_code_id,
    customer_registration_dt_id,
    customer_name,
    customer_gender,
    customer_dob,
    customer_email,
    customer_phone,
    source_system,
    source_entity,
    IS_ACTIVE,
    INSERT_DT,
    START_DT,
    END_DT
)
SELECT
    NEXTVAL('ce_customers_seq'),
    customer_src_id,
    customer_zip_code_id,
    customer_registration_dt_id,
    customer_name,
    customer_gender,
    customer_dob,
    customer_email,
    customer_phone,
    source_system,
    source_entity,
    true,
    CURRENT_DATE,
    CURRENT_DATE,
    '9999-12-31'::DATE
    
FROM mapped_customers;


-- ======================================
-- Sales
-- ======================================

WITH scratch_sales AS (
    SELECT
        game_number AS game_number_src_id,
        customer_id AS customer_src_id,
        employee_id AS employee_src_id,
        retailer_license_number AS retailer_license_number_src_id,
        payment_method_id AS payment_method_src_id,
        transaction_dt_id,
        tickets_bought,
        payout,
        sales,
        ticket_price,
        'sa_final_scratch' AS source_system,
        'src_final_scratch' AS source_entity
    FROM sa_final_scratch.src_final_scratch
    WHERE customer_id IS NOT NULL
),

draw_sales AS (
    SELECT
        game_number AS game_number_src_id,
        customer_id AS customer_src_id,
        employee_id AS employee_src_id,
        retailer_license_number AS retailer_license_number_src_id,
        payment_method_id AS payment_method_src_id,
        transaction_dt_id,
        tickets_bought,
        payout,
        sales,
        ticket_price,
        'sa_final_draw' AS source_system,
        'src_final_draw' AS source_entity
    FROM sa_final_draw.src_final_draw
    WHERE customer_id IS NOT NULL
),

stg_sales AS (
    SELECT * FROM scratch_sales
    UNION ALL
    SELECT * FROM draw_sales
)

INSERT INTO bl_3nf.ce_sales (
    game_number_id,
    customer_id,
    employee_id,
    retailer_license_number_id,
    payment_id,
    transaction_dt_id,
    tickets_bought,
    payout,
    sales,
    ticket_price,
    insert_dt,
    update_dt
)
SELECT
    COALESCE(gn.game_number_id, -1) AS game_number_id,
    COALESCE(c.customer_id, -1) AS customer_id,
    COALESCE(e.employee_id, -1) AS employee_id,
    COALESCE(r.retailer_license_number_id, -1) AS retailer_license_number_id,
    COALESCE(p.payment_method_id, -1) AS payment_id,
    COALESCE(CAST(s.transaction_dt_id AS BIGINT), -1),
    COALESCE(CAST(s.tickets_bought AS INT), -1),
    COALESCE(CAST(s.payout AS FLOAT), -1),
    COALESCE(CAST(s.sales AS FLOAT), -1),
    COALESCE(CAST(s.ticket_price AS FLOAT), -1),
    CURRENT_DATE::DATE,
    CURRENT_DATE::DATE
FROM stg_sales s
LEFT JOIN bl_3nf.ce_game_numbers gn ON s.game_number_src_id = gn.game_number_src_id
LEFT JOIN bl_3nf.ce_customers_scd c ON s.customer_src_id = c.customer_src_id
LEFT JOIN bl_3nf.ce_employees e ON s.employee_src_id = e.employee_src_id
LEFT JOIN bl_3nf.ce_retailer_license_numbers r ON s.retailer_license_number_src_id = r.retailer_license_number_src_id
LEFT JOIN bl_3nf.ce_payment_methods p ON s.payment_method_src_id = p.payment_method_src_id;






