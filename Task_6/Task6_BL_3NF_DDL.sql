-- ======================================
-- Optional: Drop the tables if exist and are populated by other values in the schema
-- ======================================
DROP TABLE IF EXISTS bl_3nf.ce_cities CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_customers_scd CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_departments CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_employees CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_game_categories CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_game_numbers CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_game_types CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_location_names CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_payment_methods CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_retailer_license_numbers CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_sales CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_states CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_statuses CASCADE;
DROP TABLE IF EXISTS bl_3nf.ce_zip CASCADE;

-- ======================================
-- Schema: bl_3nf
-- ======================================
CREATE SCHEMA IF NOT EXISTS bl_3nf;

-- ======================================
-- Table: bl_3nf.CE_GAME_TYPES
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_GAME_TYPES (
    GAME_TYPE_ID BIGINT PRIMARY KEY,
    GAME_TYPE_SRC_ID VARCHAR(255) UNIQUE,
    GAME_TYPE_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_GAME_CATEGORIES
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_GAME_CATEGORIES (
    GAME_CATEGORY_ID BIGINT PRIMARY key ,
    GAME_CATEGORY_SRC_ID VARCHAR(255) UNIQUE,
    GAME_TYPE_ID BIGINT REFERENCES bl_3nf.CE_GAME_TYPES(GAME_TYPE_ID) ON DELETE CASCADE,
    GAME_CATEGORY_NAME VARCHAR(100),
    WINNING_CHANCE FLOAT,
    WINNING_JACKPOT FLOAT,
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_GAME_NUMBERS
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_GAME_NUMBERS (
    GAME_NUMBER_ID BIGINT PRIMARY KEY,
    GAME_NUMBER_SRC_ID VARCHAR(255) UNIQUE,
    GAME_CATEGORY_ID BIGINT REFERENCES bl_3nf.CE_GAME_CATEGORIES(GAME_CATEGORY_ID) ON DELETE CASCADE,
    DRAW_DT DATE,
    GAME_NUMBER_NAME VARCHAR(100),
    AVERAGE_ODDS VARCHAR(30),
    AVERAGE_ODDS_PROB FLOAT,
    MID_TIER_PRIZE FLOAT,
    TOP_TIER_PRIZE FLOAT,
    SMALL_PRIZE FLOAT,
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_PAYMENT_METHODS
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_PAYMENT_METHODS (
    PAYMENT_METHOD_ID BIGINT PRIMARY KEY,
    PAYMENT_METHOD_SRC_ID VARCHAR(255) UNIQUE,
    PAYMENT_METHOD_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_STATES
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_STATES (
    STATE_ID BIGINT PRIMARY KEY,
    STATE_SRC_ID VARCHAR(255) UNIQUE,
    STATE_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_CITIES
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_CITIES (
    CITY_ID BIGINT PRIMARY KEY,
    CITY_SRC_ID VARCHAR(255) UNIQUE,
    STATE_ID BIGINT REFERENCES bl_3nf.CE_STATES(STATE_ID) ON DELETE CASCADE,
     CITY_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_ZIP
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_ZIP (
    ZIP_ID BIGINT PRIMARY KEY,
    ZIP_SRC_ID VARCHAR(255) UNIQUE,
    CITY_ID BIGINT REFERENCES bl_3nf.CE_CITIES(CITY_ID) ON DELETE CASCADE,
    ZIP_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_LOCATION_NAMES
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_LOCATION_NAMES (
    LOCATION_NAME_ID BIGINT PRIMARY KEY,
    LOCATION_NAME_SRC_ID VARCHAR(255) UNIQUE,
    ZIP_ID BIGINT REFERENCES bl_3nf.CE_ZIP(ZIP_ID) ON DELETE CASCADE,
    LOCATION_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_RETAILER_LICENSE_NUMBERS
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_RETAILER_LICENSE_NUMBERS (
    RETAILER_LICENSE_NUMBER_ID BIGINT PRIMARY KEY,
    RETAILER_LICENSE_NUMBER_SRC_ID VARCHAR(255) UNIQUE,
    RETAILER_LOCATION_NAME_ID BIGINT REFERENCES bl_3nf.CE_LOCATION_NAMES(LOCATION_NAME_ID) ON DELETE CASCADE,
    RETAILER_LICENSE_NUMBER_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
    
);

-- ======================================
-- Table: bl_3nf.CE_STATUSES
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_STATUSES (
    STATUS_ID BIGINT PRIMARY KEY,
    STATUS_SRC_ID VARCHAR(255) UNIQUE,
    STATUS_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_DEPARTMENTS
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_DEPARTMENTS (
    DEPARTMENT_ID BIGINT PRIMARY KEY,
    DEPARTMENT_SRC_ID VARCHAR(255) UNIQUE,
    DEPARTMENT_NAME VARCHAR(100),

    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_EMPLOYEES
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_EMPLOYEES (
    EMPLOYEE_ID BIGINT PRIMARY KEY,
    EMPLOYEE_SRC_ID VARCHAR(255) UNIQUE,
    EMPLOYEE_DEPARTMENT_ID BIGINT REFERENCES bl_3nf.CE_DEPARTMENTS(DEPARTMENT_ID) ON DELETE CASCADE,
    EMPLOYEE_STATUS_ID BIGINT REFERENCES bl_3nf.CE_STATUSES(STATUS_ID) ON DELETE CASCADE,
    EMPLOYEE_HIRE_DT DATE,
    EMPLOYEE_NAME VARCHAR(100),
    EMPLOYEE_EMAIL VARCHAR(100),
    EMPLOYEE_PHONE VARCHAR(50),
    EMPLOYEE_SALARY FLOAT,
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Table: bl_3nf.CE_CUSTOMERS_SCD
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_CUSTOMERS_SCD (
    CUSTOMER_ID BIGINT,
    CUSTOMER_SRC_ID VARCHAR(255) UNIQUE,
    CUSTOMER_ZIP_CODE_ID BIGINT REFERENCES bl_3nf.CE_ZIP(ZIP_ID) ON DELETE CASCADE,
    CUSTOMER_REGISTRATION_DT DATE,
    CUSTOMER_NAME VARCHAR(100),
    CUSTOMER_GENDER VARCHAR(20),
    CUSTOMER_DOB DATE,
    CUSTOMER_EMAIL VARCHAR(100),
    CUSTOMER_PHONE VARCHAR(50),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    IS_ACTIVE BOOLEAN,
    INSERT_DT DATE,
    START_DT DATE,
    END_DT DATE,
    PRIMARY KEY (CUSTOMER_ID, START_DT)
);

-- ======================================
-- Table: bl_3nf.CE_SALES
-- ======================================
CREATE TABLE IF NOT EXISTS bl_3nf.CE_SALES (
    GAME_NUMBER_ID BIGINT REFERENCES bl_3nf.CE_GAME_NUMBERS(GAME_NUMBER_ID) ON DELETE CASCADE,
    CUSTOMER_ID BIGINT, -- the relationship is only logical
    EMPLOYEE_ID BIGINT REFERENCES bl_3nf.CE_EMPLOYEES(EMPLOYEE_ID) ON DELETE CASCADE,
    RETAILER_LICENSE_NUMBER_ID BIGINT REFERENCES bl_3nf.CE_RETAILER_LICENSE_NUMBERS(RETAILER_LICENSE_NUMBER_ID) ON DELETE CASCADE,
    PAYMENT_ID BIGINT REFERENCES bl_3nf.CE_PAYMENT_METHODS(PAYMENT_METHOD_ID) ON DELETE CASCADE,
    EVENT_DT DATE,
    TICKETS_BOUGHT INT,
    PAYOUT FLOAT,
    SALES FLOAT,
    TICKET_PRICE FLOAT,
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- ======================================
-- Default Rows
-- ======================================

-- ======================================
-- Game Types
-- ======================================
INSERT INTO bl_3nf.ce_game_types VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Game Categories
-- ======================================
INSERT INTO bl_3nf.ce_game_categories VALUES (-1, 'n. a.', -1,'n. a.', -1, -1, 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Game Numbers
-- ======================================
INSERT INTO bl_3nf.ce_game_numbers VALUES (
  -1, 'n. a.', -1, DATE '1900-01-01','n. a.', 'n. a.', -1, -1, -1, -1,
  'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01'
);
COMMIT;

-- ======================================
-- Payment Methods
-- ======================================
INSERT INTO bl_3nf.ce_payment_methods VALUES (-1, 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- States
-- ======================================
INSERT INTO bl_3nf.ce_states VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Cities
-- ======================================
INSERT INTO bl_3nf.ce_cities VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Zip Codes
-- ======================================
INSERT INTO bl_3nf.ce_zip VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Location Names
-- ======================================
INSERT INTO bl_3nf.ce_location_names VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Retailer License Numbers
-- ======================================
INSERT INTO bl_3nf.ce_retailer_license_numbers VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Statuses
-- ======================================
INSERT INTO bl_3nf.ce_statuses VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Departments
-- ======================================
INSERT INTO bl_3nf.ce_departments VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Employees
-- ======================================
INSERT INTO bl_3nf.ce_employees VALUES (
  -1, 'n. a.', -1, -1, DATE '1900-01-01', 'n. a.', 'n. a.', 'n. a.', -1,
  'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01'
);
COMMIT;

-- ======================================
-- Customers SCD
-- ======================================
INSERT INTO bl_3nf.ce_customers_scd VALUES (
  -1, 'n. a.', -1, DATE '1900-01-01', 'n. a.', 'n. a.', DATE '1900-01-01', 'n. a.', 'n. a.',
  'MANUAL', 'MANUAL', TRUE, DATE '1900-01-01', DATE '1900-01-01', DATE '9999-12-31'
);
COMMIT;
