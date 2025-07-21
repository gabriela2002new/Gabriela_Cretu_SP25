create schema if not exists bl_3nf;

-- ======================================
-- Game Types
-- ======================================
drop table if exists bl_3nf.ce_game_types cascade;
CREATE TABLE IF NOT EXISTS bl_3nf.ce_game_types (
    GAME_TYPE_ID     BIGINT PRIMARY KEY,
    GAME_TYPE_SRC_ID VARCHAR(255) UNIQUE,
    GAME_TYPE_NAME   VARCHAR(100),
    SOURCE_SYSTEM    VARCHAR(30),
    SOURCE_ENTITY    VARCHAR(30),
    INSERT_DT        DATE,
    UPDATE_DT        DATE
);

INSERT INTO bl_3nf.ce_game_types VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;
-- ======================================
-- Game Categories
-- ======================================
drop table if exists bl_3nf.CE_GAME_CATEGORIES cascade;

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

INSERT INTO bl_3nf.ce_game_categories VALUES (-1, 'n. a.', -1,'n. a.', -1, -1, 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;


-- ======================================
-- Game Numbers
-- ======================================
drop table if exists bl_3nf.CE_GAME_NUMBERS cascade;


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

INSERT INTO bl_3nf.ce_game_numbers VALUES (
  -1, 'n. a.', -1, DATE '1900-01-01','n. a.', 'n. a.', -1, -1, -1, -1,
  'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01'
);
COMMIT;

-- ======================================
-- Payment Methods
-- ======================================

drop table if exists bl_3nf.CE_PAYMENT_METHODS cascade;

CREATE TABLE IF NOT EXISTS bl_3nf.CE_PAYMENT_METHODS (
    PAYMENT_METHOD_ID BIGINT PRIMARY KEY,
    PAYMENT_METHOD_SRC_ID VARCHAR(255) UNIQUE,
    PAYMENT_METHOD_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);



INSERT INTO bl_3nf.ce_payment_methods VALUES (-1, 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- States
-- ======================================
drop table if exists bl_3nf.CE_STATES cascade;


CREATE TABLE IF NOT EXISTS bl_3nf.CE_STATES (
    STATE_ID BIGINT PRIMARY KEY,
    STATE_SRC_ID VARCHAR(255) UNIQUE,
    STATE_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

INSERT INTO bl_3nf.ce_states VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Cities
-- ======================================

drop table if exists bl_3nf.CE_CITIES cascade;


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
INSERT INTO bl_3nf.ce_cities VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;





-- ======================================
-- Zip Codes
-- ======================================
drop table if exists  bl_3nf.CE_ZIP cascade;
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

INSERT INTO bl_3nf.ce_zip VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Location Names
-- ======================================
drop table if exists bl_3nf.CE_LOCATION_NAMES cascade;
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
INSERT INTO bl_3nf.ce_location_names VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Retailer License Numbers
-- ======================================
drop table if exists bl_3nf.CE_RETAILER_LICENSE_NUMBERS cascade;

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

INSERT INTO bl_3nf.ce_retailer_license_numbers VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Statuses
-- ======================================
drop table if exists bl_3nf.CE_STATUSES cascade;

CREATE TABLE IF NOT EXISTS bl_3nf.CE_STATUSES (
    STATUS_ID BIGINT PRIMARY KEY,
    STATUS_SRC_ID VARCHAR(255) UNIQUE,
    STATUS_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);
INSERT INTO bl_3nf.ce_statuses VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Departments
-- ======================================
drop table if exists bl_3nf.ce_departments cascade;
CREATE TABLE IF NOT EXISTS bl_3nf.CE_DEPARTMENTS (
    DEPARTMENT_ID BIGINT PRIMARY KEY,
    DEPARTMENT_SRC_ID VARCHAR(255) UNIQUE,
    DEPARTMENT_NAME VARCHAR(100),

    SOURCE_SYSTEM VARCHAR(30),
    SOURCE_ENTITY VARCHAR(30),
    INSERT_DT DATE,
    UPDATE_DT DATE
);
INSERT INTO bl_3nf.ce_departments VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01');
COMMIT;

-- ======================================
-- Employees
-- ======================================
drop table if exists bl_3nf.CE_EMPLOYEES cascade;

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
INSERT INTO bl_3nf.ce_employees VALUES (
  -1, 'n. a.', -1, -1, DATE '1900-01-01', 'n. a.', 'n. a.', 'n. a.', -1,
  'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01'
);
COMMIT;

-- ======================================
-- Customers SCD
-- ======================================

drop table if exists bl_3nf.CE_CUSTOMERS_SCD cascade;

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
INSERT INTO bl_3nf.ce_customers_scd VALUES (
  -1, 'n. a.', -1, DATE '1900-01-01', 'n. a.', 'n. a.', DATE '1900-01-01', 'n. a.', 'n. a.',
  'MANUAL', 'MANUAL', TRUE, DATE '1900-01-01', DATE '1900-01-01', DATE '9999-12-31'
);
COMMIT;

-- ======================================
-- Sales
-- ======================================
drop table if exists bl_3nf.CE_SALES;
CREATE TABLE IF NOT EXISTS bl_3nf.CE_SALES (
    GAME_NUMBER_ID BIGINT NOT NULL
        REFERENCES bl_3nf.CE_GAME_NUMBERS(GAME_NUMBER_ID) ON DELETE CASCADE,

    CUSTOMER_ID BIGINT NOT NULL, -- Logical relationship; no FK constraint

    EMPLOYEE_ID BIGINT NOT NULL
        REFERENCES bl_3nf.CE_EMPLOYEES(EMPLOYEE_ID) ON DELETE CASCADE,

    RETAILER_LICENSE_NUMBER_ID BIGINT NOT NULL
        REFERENCES bl_3nf.CE_RETAILER_LICENSE_NUMBERS(RETAILER_LICENSE_NUMBER_ID) ON DELETE CASCADE,

    PAYMENT_ID BIGINT NOT NULL
        REFERENCES bl_3nf.CE_PAYMENT_METHODS(PAYMENT_METHOD_ID) ON DELETE CASCADE,

    EVENT_DT DATE NOT NULL,

    TICKETS_BOUGHT INT CHECK (TICKETS_BOUGHT >= 0),
    PAYOUT FLOAT CHECK (PAYOUT >= 0),
    SALES FLOAT CHECK (SALES >= 0),
    TICKET_PRICE FLOAT CHECK (TICKET_PRICE >= 0),

    INSERT_DT DATE NOT NULL,
    UPDATE_DT DATE,

    -- Composite primary key defines the grain
    CONSTRAINT pk_ce_sales PRIMARY KEY (
        GAME_NUMBER_ID,
        CUSTOMER_ID,
        EMPLOYEE_ID,
        RETAILER_LICENSE_NUMBER_ID,
        PAYMENT_ID,
        EVENT_DT
    )
);


