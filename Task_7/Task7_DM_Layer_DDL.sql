-- ======================================
-- Drop Tables
-- ======================================
DROP TABLE IF EXISTS bl_dm.FCT_SALES CASCADE;
DROP TABLE IF EXISTS bl_dm.DIM_GAME_NUMBERS CASCADE;
DROP TABLE IF EXISTS bl_dm.DIM_RETAILER_LICENSE_NUMBERS CASCADE;
DROP TABLE IF EXISTS bl_dm.DIM_CUSTOMERS_SCD CASCADE;
DROP TABLE IF EXISTS bl_dm.DIM_EMPLOYEES CASCADE;
DROP TABLE IF EXISTS bl_dm.DIM_PAYMENT_METHODS CASCADE;

-- ======================================
-- Schema
-- ======================================
CREATE SCHEMA IF NOT EXISTS bl_dm;



-- ======================================
-- Table: DIM_GAME_NUMBERS
-- ======================================
CREATE TABLE IF NOT EXISTS bl_dm.dim_game_numbers (
    game_number_surr_id BIGINT PRIMARY KEY,
    game_number_src_id VARCHAR(255) UNIQUE,
    game_number_name VARCHAR(100),
    game_category_id BIGINT,
    game_category_name VARCHAR(100),
    game_type_id BIGINT,
    game_type_name VARCHAR(100),
    draw_dt DATE ,
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

-- ======================================
-- Table: DIM_RETAILER_LICENSE_NUMBERS
-- ======================================
CREATE TABLE IF NOT EXISTS bl_dm.dim_retailer_license_numbers(
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

-- ======================================
-- Table: DIM_CUSTOMERS_SCD
-- ======================================
CREATE TABLE IF NOT EXISTS bl_dm.dim_customers_scd (
    customer_surr_id BIGINT primary KEY,
    customer_src_id VARCHAR(255) UNIQUE,
    customer_name VARCHAR(100),
    customer_registration_dt DATE ,
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
    is_active BOOLEAN
   );

-- ======================================
-- Table: DIM_EMPLOYEES
-- ======================================
CREATE TABLE IF NOT EXISTS bl_dm.dim_employees(
    EMPLOYEE_SURR_ID BIGINT PRIMARY KEY,
    EMPLOYEE_SRC_ID VARCHAR(255) UNIQUE,
    EMPLOYEE_NAME VARCHAR(100),
    EMPLOYEE_HIRE_DT DATE ,
    EMPLOYEE_STATUS_ID BIGINT,
    EMPLOYEE_STATUS_NAME VARCHAR(100),
    EMPLOYEE_DEPARTMENT_ID BIGINT,
    EMPLOYEE_DEPARTMENT_NAME VARCHAR(100),
    EMPLOYEE_EMAIL VARCHAR(100),
    EMPLOYEE_PHONE VARCHAR(50),
    EMPLOYEE_SALARY FLOAT,
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: DIM_PAYMENT_METHODS
-- ======================================
CREATE TABLE IF NOT EXISTS bl_dm.dim_payment_methods (
    PAYMENT_METHOD_SURR_ID BIGINT PRIMARY KEY,
    PAYMENT_METHOD_SRC_ID VARCHAR(255) UNIQUE,
    PAYMENT_METHOD_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: FCT_SALES
-- ======================================
CREATE TABLE IF NOT EXISTS bl_dm.fct_sales (
    game_number_surr_id BIGINT REFERENCES bl_dm.dim_game_numbers(game_number_surr_id) ON DELETE CASCADE,
    customer_surr_id BIGINT REFERENCES bl_dm.dim_customer_scd(customer_surr_id) ON DELETE CASCADE,
    employee_surr_id BIGINT REFERENCES bl_dm.dim_employees(employee_surr_id) ON DELETE CASCADE,
    retailer_license_number_surr_id BIGINT REFERENCES bl_dm.dim_retailer_license_numbers(retailer_license_number_surr_id) ON DELETE CASCADE,
    payment_method_surr_id BIGINT REFERENCES bl_dm.dim_payment_methods(payment_method_surr_id) ON DELETE CASCADE,
    event_dt DATE REFERENCES bl_dm.dim_date(event_dt) ON DELETE CASCADE,
    ticket_price FLOAT,
    tickets_bought INT,
    payout FLOAT,
    sales FLOAT,
    insert_dt DATE,
    update_dt DATE
);

-- ======================================
-- Default Rows
-- ======================================

-- DIM_DATE
INSERT INTO bl_dm.DIM_DATE VALUES (DATE '1900-01-01', -1, -1, 'n. a.', -1, -1, 'n. a.', DATE '1900-01-01');
COMMIT;

-- DIM_GAME_NUMBERS
INSERT INTO bl_dm.DIM_GAME_NUMBERS VALUES (
  -1, 'n. a.', 'n. a.', -1, 'n. a.', -1, 'n. a.', DATE '1900-01-01',
  'n. a.', -1, -1, -1, -1, -1, -1,
  'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01'
);
COMMIT;

-- DIM_RETAILER_LICENSE_NUMBERS
INSERT INTO bl_dm.DIM_RETAILER_LICENSE_NUMBERS VALUES (
  -1, 'n. a.', 'n. a.', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.',
  'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01'
);
COMMIT;

-- DIM_CUSTOMERS_SCD
INSERT INTO bl_dm.DIM_CUSTOMERS_SCD VALUES (
  -1, 'n. a.', 'n. a.', DATE '1900-01-01', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.',
  'n. a.', DATE '1900-01-01', 'n. a.', 'n. a.', 'MANUAL', 'MANUAL',
  DATE '1900-01-01', DATE '1900-01-01', DATE '9999-12-31', TRUE
);
COMMIT;

-- DIM_EMPLOYEES
INSERT INTO bl_dm.DIM_EMPLOYEES VALUES (
  -1, 'n. a.', 'n. a.', DATE '1900-01-01', -1, 'n. a.', -1, 'n. a.',
  'n. a.', 'n. a.', -1, 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01'
);
COMMIT;

-- DIM_PAYMENT_METHODS
INSERT INTO bl_dm.DIM_PAYMENT_METHODS VALUES (
  -1, 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01'
);
COMMIT;
