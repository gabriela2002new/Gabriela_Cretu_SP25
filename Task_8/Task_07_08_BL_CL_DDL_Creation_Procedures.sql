CREATE OR REPLACE PROCEDURE bl_cl.create_bl_3nf_sequences()
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE SEQUENCE IF NOT EXISTS bl_cl.t_map_game_types_seq START 1;
    CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_game_categories_seq START 1;
    CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_game_numbers_seq START 1;
    CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_payment_methods_seq START 1;
    CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_states_seq START 1;
    CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_cities_seq START 1;
    CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_zips_seq START 1;
    CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_location_names_seq START 1;
    CREATE SEQUENCE IF NOT EXISTS bl_cl.t_mapping_retailer_license_seq START 1;
    CREATE SEQUENCE IF NOT EXISTS bl_cl.t_mapping_status_seq START 1;
    CREATE SEQUENCE IF NOT EXISTS bl_cl.t_mapping_department_seq START 1;
    CREATE SEQUENCE IF not EXISTS bl_cl.t_mapping_employees_seq start 1;
    CREATE SEQUENCE IF not EXISTS bl_cl.t_mapping_customers_seq start 1;
    RAISE NOTICE 'All sequences created successfully.';
END;
$$;



CREATE OR REPLACE PROCEDURE bl_cl.create_bl_cl_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ======================================
-- Game Types
-- ======================================


CREATE TABLE if not exists bl_cl.mta_game_types (
    column_name            VARCHAR,
    source_column_name     VARCHAR,
    data_type              VARCHAR,
    transformation_rule    TEXT,
    is_nullable            BOOLEAN,
    notes                  TEXT
);



CREATE TABLE if not exists bl_cl.wrk_game_types (
    game_type_src_id   VARCHAR,
    game_type_name     VARCHAR,
    source_system      VARCHAR,
    source_entity      VARCHAR,
    load_dt            DATE
);

CREATE table if not exists bl_cl.lkp_game_types (
    game_type_id       INT ,
    game_type_src_id   VARCHAR,
    game_type_name     VARCHAR,
    source_system      VARCHAR,
    source_entity      VARCHAR,
    insert_dt          DATE,
    update_dt          DATE,
    CONSTRAINT uq_game_type UNIQUE (game_type_src_id, source_system, source_entity)
);

-- ======================================
-- Game Categories
-- ======================================

CREATE TABLE IF NOT EXISTS bl_cl.mta_game_categories (
    column_name            VARCHAR,
    source_column_name     VARCHAR,
    data_type              VARCHAR,
    transformation_rule    TEXT,
    is_nullable            BOOLEAN,
    notes                  TEXT
);



CREATE table if not exists bl_cl.wrk_game_categories (
    game_category_src_id VARCHAR,
    game_type_name       VARCHAR,
    winning_chance       FLOAT,
    winning_jackpot      FLOAT,
    source_system        VARCHAR,
    source_entity        VARCHAR,
    load_dt              DATE
);

CREATE TABLE IF NOT EXISTS bl_cl.lkp_game_categories (
    game_category_id      BIGINT,
    game_category_src_id  VARCHAR,
    game_category_name    VARCHAR,
    game_type_name        VARCHAR,
    winning_chance        FLOAT,
    winning_jackpot       FLOAT,
    source_system         VARCHAR,
    source_entity         VARCHAR,
    insert_dt             DATE,
    update_dt             DATE,
    CONSTRAINT uq_game_category UNIQUE (game_category_src_id, source_system, source_entity)
);


-- ======================================
-- Game Numbers
-- ======================================


CREATE table if not exists bl_cl.mta_game_numbers (
    column_name       VARCHAR(100) NOT NULL,
    source_column_name     VARCHAR(100) NOT NULL,
    data_type         VARCHAR(50) NOT NULL,
    transformation_rule    VARCHAR(255) NOT NULL,
    is_nullable          BOOLEAN NOT NULL,
    notes       VARCHAR(255) NOT NULL
);


-- Working (staging) table to hold source data before mapping
CREATE TABLE if not exists bl_cl.wrk_game_numbers (
    game_number_src_id VARCHAR(255) NOT NULL,
    game_category_name VARCHAR(255),
    draw_dt DATE,
    average_odds VARCHAR(30),
    average_odds_prob FLOAT,
    mid_tier_prize FLOAT,
    top_tier_prize FLOAT,
    small_prize FLOAT,
    source_system VARCHAR(30) NOT NULL,
    source_entity VARCHAR(30) NOT NULL,
    load_dt DATE NOT NULL
);


-- Lookup (final mapping) table with surrogate keys and unique constraint
CREATE table if not exists bl_cl.lkp_game_numbers (
    game_number_id BIGINT PRIMARY KEY,
    game_number_src_id VARCHAR(255) NOT NULL,
    game_number_name VARCHAR(255) NOT NULL,
    game_category_name VARCHAR(255),
    draw_dt DATE,
    average_odds VARCHAR(30),
    average_odds_prob FLOAT,
    mid_tier_prize FLOAT,
    top_tier_prize FLOAT,
    small_prize FLOAT,
    source_system VARCHAR(30) NOT NULL,
    source_entity VARCHAR(30) NOT NULL,
    insert_dt DATE NOT NULL,
    update_dt DATE NOT NULL,
    UNIQUE (game_number_src_id, source_system, source_entity)
);


-- ======================================
-- Payment Methods
-- ======================================


CREATE table if not exists bl_cl.mta_payment_methods (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT NULL
);


CREATE table if not exists bl_cl.wrk_payment_methods (
    payment_method_src_id   VARCHAR(255) NOT NULL,
    payment_method_name     VARCHAR(100),
    source_system           VARCHAR(30) NOT NULL,
    source_entity           VARCHAR(30) NOT NULL,
    load_dt               DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS bl_cl.lkp_payment_methods (
    payment_method_id     BIGINT NOT NULL,
    payment_method_src_id VARCHAR(255) NOT NULL,
    payment_method_name   VARCHAR(100),
    source_system         VARCHAR(30) NOT NULL,
    source_entity         VARCHAR(30) NOT NULL,
    insert_dt             DATE NOT NULL,
    update_dt             DATE NOT NULL,
    CONSTRAINT unq_payment_method_triplet UNIQUE (payment_method_src_id, source_system, source_entity)
);



-- ======================================
-- States
-- ======================================


CREATE TABLE if not exists bl_cl.mta_states (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT NULL
);


CREATE table if not exists bl_cl.wrk_states (
    state_src_id   VARCHAR(255) NOT NULL,
    source_system  VARCHAR(30) NOT NULL,
    source_entity  VARCHAR(30) NOT NULL,
    load_dt      DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS bl_cl.lkp_states (
    state_id        BIGINT NOT NULL,
    state_src_id    VARCHAR(255) NOT NULL,
    state_name      VARCHAR(100),
    source_system   VARCHAR(30) NOT NULL,
    source_entity   VARCHAR(30) NOT NULL,
    insert_dt       DATE NOT NULL,
    update_dt       DATE NOT NULL,
    CONSTRAINT unq_state_triplet UNIQUE (state_src_id, source_system, source_entity)
);




-- ======================================
-- Cities
-- ======================================


CREATE table if not exists bl_cl.mta_cities (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT NULL
);


CREATE table if not exists  bl_cl.wrk_cities (
    city_src_id     VARCHAR(255) NOT NULL,
    state_name      VARCHAR(100),
    source_system   VARCHAR(30) NOT NULL,
    source_entity   VARCHAR(30) NOT NULL,
    load_dt       DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS bl_cl.lkp_cities (
    city_id         BIGINT NOT NULL,
    city_src_id     VARCHAR(255) NOT NULL,
    state_name      VARCHAR(100),
    city_name       VARCHAR(100),
    source_system   VARCHAR(30) NOT NULL,
    source_entity   VARCHAR(30) NOT NULL,
    insert_dt       DATE NOT NULL,
    update_dt       DATE NOT NULL,
    CONSTRAINT unq_city_triplet UNIQUE (city_src_id, source_system, source_entity)
);


-- ======================================
-- Zipcodes
-- ======================================



CREATE table if not exists  bl_cl.mta_zips (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT NULL
);



CREATE table if not exists  bl_cl.wrk_zips (
    zip_src_id     VARCHAR(20) NOT NULL,
    city_name      VARCHAR(100),
    source_system  VARCHAR(30) NOT NULL,
    source_entity  VARCHAR(30) NOT NULL,
    load_dt      DATE NOT NULL
);


CREATE TABLE IF NOT EXISTS bl_cl.lkp_zips (
    zip_id         BIGINT NOT NULL,
    zip_src_id     VARCHAR(20) NOT NULL,
    city_name      VARCHAR(100),
    zip_name       VARCHAR(20) NOT NULL,
    source_system  VARCHAR(30) NOT NULL,
    source_entity  VARCHAR(30) NOT NULL,
    insert_dt      DATE NOT NULL,
    update_dt      DATE NOT NULL,
    
    CONSTRAINT unq_zip_triplet UNIQUE (zip_src_id, source_system, source_entity)
);



-- ======================================
-- Location Names
-- ======================================


CREATE table if not exists bl_cl.mta_location_names (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT NULL
);



CREATE table if not exists bl_cl.wrk_location_names (
    location_name_src_id VARCHAR(255) NOT NULL,
    zip_name           VARCHAR(20)  NOT NULL,
    source_system        VARCHAR(30)  NOT NULL,
    source_entity        VARCHAR(30)  NOT NULL,
    load_dt            DATE         NOT NULL
);


CREATE table if not exists bl_cl.lkp_location_names (
    location_name_id      BIGINT      NOT NULL,
    location_name_src_id  VARCHAR(255) NOT NULL,
    zip_name              VARCHAR(20) NOT NULL,
    location_name         VARCHAR(255) NOT NULL,
    source_system         VARCHAR(30) NOT NULL,
    source_entity         VARCHAR(30) NOT NULL,
    insert_dt             DATE       NOT NULL,
    update_dt             DATE       NOT NULL,
    
    CONSTRAINT unq_location_name_triplet UNIQUE (location_name_src_id, source_system, source_entity)
);


-- ======================================
-- Retailers
-- ======================================


CREATE TABLE if not exists bl_cl.mta_retailers (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT NULL
);


CREATE TABLE if not exists bl_cl.wrk_retailers (
    retailer_license_number_src_id  VARCHAR(100) NOT NULL,
    location_name                   VARCHAR(255),
    source_system                   VARCHAR(30) NOT NULL,
    source_entity                   VARCHAR(30) NOT NULL,
    load_dt                       DATE NOT NULL
);



CREATE table if not exists bl_cl.lkp_retailers (
    retailer_license_number_id      BIGINT NOT NULL,
    retailer_license_number_src_id  VARCHAR(100) NOT NULL,
    location_name                   VARCHAR(255),
    retailer_license_number_name    VARCHAR(100) NOT NULL,
    source_system                   VARCHAR(30) NOT NULL,
    source_entity                   VARCHAR(30) NOT NULL,
    insert_dt                       DATE NOT NULL,
    update_dt                       DATE NOT NULL,

    CONSTRAINT unq_retailer_triplet UNIQUE (retailer_license_number_src_id, source_system, source_entity)
);



-- ======================================
-- Statuses
-- ======================================

CREATE table if not exists bl_cl.mta_statuses (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT NULL
);


CREATE table if not exists bl_cl.wrk_statuses (
    status_src_id VARCHAR(100) NOT NULL,
    source_system VARCHAR(30) NOT NULL,
    source_entity VARCHAR(30) NOT NULL,
    load_dt     DATE NOT NULL
);



CREATE TABLE IF NOT EXISTS bl_cl.lkp_statuses (
    status_id       BIGINT ,
    status_src_id   VARCHAR(100) NOT NULL,
    status_name     VARCHAR(100) NOT NULL,
    source_system   VARCHAR(30) NOT NULL,
    source_entity   VARCHAR(30) NOT NULL,
    insert_dt       DATE NOT NULL,
    update_dt       DATE NOT NULL,

    CONSTRAINT unq_status_triplet UNIQUE (status_src_id, source_system, source_entity)
);



-- ======================================
-- Departments
-- ======================================


CREATE table if not exists bl_cl.mta_departments (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT NULL
);




CREATE TABLE if not exists bl_cl.wrk_departments (
    department_src_id VARCHAR(100) NOT NULL,
    source_system VARCHAR(30) NOT NULL,
    source_entity VARCHAR(30) NOT NULL,
    load_dt DATE NOT NULL
);


CREATE TABLE IF NOT EXISTS bl_cl.lkp_departments (
    department_id BIGINT ,
    department_src_id VARCHAR(100) NOT NULL,
    department_name VARCHAR(100) NOT NULL,
    source_system VARCHAR(30) NOT NULL,
    source_entity VARCHAR(30) NOT NULL,
    insert_dt DATE NOT NULL,
    update_dt DATE NOT NULL,

    CONSTRAINT unq_department_triplet UNIQUE (department_src_id, source_system, source_entity)
);



-- ======================================
-- Employees
-- ======================================

CREATE TABLE IF NOT EXISTS bl_cl.mta_employees (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT NULL
);



CREATE TABLE if not exists bl_cl.wrk_employees (
    employee_src_id VARCHAR(255) NOT NULL,
    employee_name VARCHAR(100),
    employee_department_name VARCHAR(100),
    employee_status_name VARCHAR(100),
    employee_email VARCHAR(255),
    employee_phone VARCHAR(50),
    employee_hire_dt DATE,
    employee_salary FLOAT,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    load_dt DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS bl_cl.lkp_employees (
    employee_id BIGINT,
    employee_src_id VARCHAR(255) NOT NULL,
    employee_name VARCHAR(100) NOT NULL,
    employee_department_name VARCHAR(100) NOT NULL,
    employee_status_name VARCHAR(100) NOT NULL,
    employee_email VARCHAR(255) NOT NULL,
    employee_phone VARCHAR(50) NOT NULL,
    employee_hire_dt DATE NOT NULL,
    employee_salary FLOAT NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    insert_dt DATE NOT NULL,
    update_dt DATE NOT NULL,

    CONSTRAINT unq_employee_triplet UNIQUE (employee_src_id, source_system, source_entity)
);


-- ======================================
-- Customers
-- ======================================

CREATE TABLE IF NOT EXISTS bl_cl.mta_customers (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT null
   );


CREATE TABLE IF NOT EXISTS bl_cl.wrk_customers (
    customer_src_id           VARCHAR(100) NOT NULL,
    zip_name                  VARCHAR(20),
    customer_registration_dt  DATE,
    customer_name             VARCHAR(255),
    customer_gender           VARCHAR(10),
    customer_dob              DATE,
    customer_email            VARCHAR(255),
    customer_phone            VARCHAR(50),
    source_system             VARCHAR(100) NOT NULL,
    source_entity             VARCHAR(100) NOT NULL,
    load_dt                   DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS bl_cl.lkp_customers (
    customer_id              BIGINT ,
    customer_src_id          VARCHAR(100) NOT NULL,
    zip_name                 VARCHAR(20) DEFAULT 'n. a.',
    customer_registration_dt DATE NOT NULL DEFAULT '1900-01-01',
    customer_name            VARCHAR(255) DEFAULT 'n. a.',
    customer_gender          VARCHAR(10) DEFAULT 'n. a.',
    customer_dob             DATE NOT NULL DEFAULT '1900-01-01',
    customer_email           VARCHAR(255) DEFAULT 'n. a.',
    customer_phone           VARCHAR(50) DEFAULT 'n. a.',
    source_system            VARCHAR(100) NOT NULL,
    source_entity            VARCHAR(100) NOT NULL,
    is_active                BOOLEAN NOT NULL DEFAULT TRUE,
    insert_dt                DATE NOT NULL ,
    start_dt                 DATE NOT NULL ,
    end_dt                   DATE NOT NULL ,

    CONSTRAINT uq_lkp_customers UNIQUE (
        customer_src_id, source_system, source_entity, start_dt
    )
);


   
   
   

-- ============================================
-- Sales
-- ============================================
 
CREATE TABLE IF NOT EXISTS bl_cl.mta_sales (
    column_name      VARCHAR(100) NOT NULL,
    source_column_name    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation_rule   VARCHAR(255) NOT NULL,
    is_nullable         BOOLEAN      NOT NULL,
    notes     VARCHAR(255) NOT NULL);


CREATE TABLE if not exists bl_cl.wrk_sales (
    game_number_src_id            VARCHAR(100),
    customer_src_id               VARCHAR(100),
    employee_src_id               VARCHAR(100),
    retailer_license_number_src_id VARCHAR(100),
    payment_method_src_id         VARCHAR(100),
    transaction_dt                DATE,
    tickets_bought                INT,
    payout                        FLOAT,
    sales                         FLOAT,
    ticket_price                  FLOAT,
    source_system                 VARCHAR(100),
    source_entity                 VARCHAR(100)
);


CREATE table if not exists bl_cl.lkp_sales (
    game_number_src_id            VARCHAR(100) NOT NULL,
    customer_src_id               VARCHAR(100) NOT NULL,
    employee_src_id               VARCHAR(100) NOT NULL,
    retailer_license_number_src_id VARCHAR(100) NOT NULL,
    payment_method_src_id         VARCHAR(100) NOT NULL,
    event_dt                      DATE NOT NULL,
    tickets_bought                INT DEFAULT -1,
    payout                        FLOAT DEFAULT -1,
    sales                         FLOAT DEFAULT -1,
    ticket_price                  FLOAT DEFAULT -1,
    source_system                 VARCHAR(100) NOT NULL,
    source_entity                 VARCHAR(100) NOT NULL,
    insert_dt                     DATE NOT NULL DEFAULT CURRENT_DATE,
    update_dt                     DATE NOT NULL DEFAULT CURRENT_DATE,
    CONSTRAINT uq_sales UNIQUE (
        game_number_src_id,
        customer_src_id,
        employee_src_id,
        retailer_license_number_src_id,
        payment_method_src_id,
        event_dt,
        source_system,
        source_entity
    )
);



    RAISE NOTICE 'All tables created successfully.';
END;
$$;



CREATE OR REPLACE PROCEDURE bl_cl.create_bl_3nf_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Game Types
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_game_types (
        GAME_TYPE_ID     BIGINT PRIMARY KEY,
        GAME_TYPE_SRC_ID VARCHAR(255) UNIQUE,
        GAME_TYPE_NAME   VARCHAR(100),
        SOURCE_SYSTEM    VARCHAR(30),
        SOURCE_ENTITY    VARCHAR(30),
        INSERT_DT        DATE,
        UPDATE_DT        DATE
    );

    -- Game Categories
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_game_categories (
        GAME_CATEGORY_ID BIGINT PRIMARY KEY,
        GAME_CATEGORY_SRC_ID VARCHAR(255) UNIQUE,
        GAME_TYPE_ID BIGINT REFERENCES bl_3nf.ce_game_types(GAME_TYPE_ID) ON DELETE CASCADE,
        GAME_CATEGORY_NAME VARCHAR(100),
        WINNING_CHANCE FLOAT,
        WINNING_JACKPOT FLOAT,
        SOURCE_SYSTEM VARCHAR(30),
        SOURCE_ENTITY VARCHAR(30),
        INSERT_DT DATE,
        UPDATE_DT DATE
    );

    -- Game Numbers
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_game_numbers (
        GAME_NUMBER_ID BIGINT PRIMARY KEY,
        GAME_NUMBER_SRC_ID VARCHAR(255) UNIQUE,
        GAME_CATEGORY_ID BIGINT REFERENCES bl_3nf.ce_game_categories(GAME_CATEGORY_ID) ON DELETE CASCADE,
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

    -- Payment Methods
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_payment_methods (
        PAYMENT_METHOD_ID BIGINT PRIMARY KEY,
        PAYMENT_METHOD_SRC_ID VARCHAR(255) UNIQUE,
        PAYMENT_METHOD_NAME VARCHAR(100),
        SOURCE_SYSTEM VARCHAR(30),
        SOURCE_ENTITY VARCHAR(30),
        INSERT_DT DATE,
        UPDATE_DT DATE
    );

    -- States
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_states (
        STATE_ID BIGINT PRIMARY KEY,
        STATE_SRC_ID VARCHAR(255) UNIQUE,
        STATE_NAME VARCHAR(100),
        SOURCE_SYSTEM VARCHAR(30),
        SOURCE_ENTITY VARCHAR(30),
        INSERT_DT DATE,
        UPDATE_DT DATE
    );

    -- Cities
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_cities (
        CITY_ID BIGINT PRIMARY KEY,
        CITY_SRC_ID VARCHAR(255) UNIQUE,
        STATE_ID BIGINT REFERENCES bl_3nf.ce_states(STATE_ID) ON DELETE CASCADE,
        CITY_NAME VARCHAR(100),
        SOURCE_SYSTEM VARCHAR(30),
        SOURCE_ENTITY VARCHAR(30),
        INSERT_DT DATE,
        UPDATE_DT DATE
    );

    -- Zip Codes
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_zip (
        ZIP_ID BIGINT PRIMARY KEY,
        ZIP_SRC_ID VARCHAR(255) UNIQUE,
        CITY_ID BIGINT REFERENCES bl_3nf.ce_cities(CITY_ID) ON DELETE CASCADE,
        ZIP_NAME VARCHAR(100),
        SOURCE_SYSTEM VARCHAR(30),
        SOURCE_ENTITY VARCHAR(30),
        INSERT_DT DATE,
        UPDATE_DT DATE
    );

    -- Location Names
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_location_names (
        LOCATION_NAME_ID BIGINT PRIMARY KEY,
        LOCATION_NAME_SRC_ID VARCHAR(255) UNIQUE,
        ZIP_ID BIGINT REFERENCES bl_3nf.ce_zip(ZIP_ID) ON DELETE CASCADE,
        LOCATION_NAME VARCHAR(100),
        SOURCE_SYSTEM VARCHAR(30),
        SOURCE_ENTITY VARCHAR(30),
        INSERT_DT DATE,
        UPDATE_DT DATE
    );

    -- Retailer License Numbers
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_retailer_license_numbers (
        RETAILER_LICENSE_NUMBER_ID BIGINT PRIMARY KEY,
        RETAILER_LICENSE_NUMBER_SRC_ID VARCHAR(255) UNIQUE,
        RETAILER_LOCATION_NAME_ID BIGINT REFERENCES bl_3nf.ce_location_names(LOCATION_NAME_ID) ON DELETE CASCADE,
        RETAILER_LICENSE_NUMBER_NAME VARCHAR(100),
        SOURCE_SYSTEM VARCHAR(30),
        SOURCE_ENTITY VARCHAR(30),
        INSERT_DT DATE,
        UPDATE_DT DATE
    );

    -- Statuses
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_statuses (
        STATUS_ID BIGINT PRIMARY KEY,
        STATUS_SRC_ID VARCHAR(255) UNIQUE,
        STATUS_NAME VARCHAR(100),
        SOURCE_SYSTEM VARCHAR(30),
        SOURCE_ENTITY VARCHAR(30),
        INSERT_DT DATE,
        UPDATE_DT DATE
    );

    -- Departments
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_departments (
        DEPARTMENT_ID BIGINT PRIMARY KEY,
        DEPARTMENT_SRC_ID VARCHAR(255) UNIQUE,
        DEPARTMENT_NAME VARCHAR(100),
        SOURCE_SYSTEM VARCHAR(30),
        SOURCE_ENTITY VARCHAR(30),
        INSERT_DT DATE,
        UPDATE_DT DATE
    );

    -- Employees
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_employees (
        EMPLOYEE_ID BIGINT PRIMARY KEY,
        EMPLOYEE_SRC_ID VARCHAR(255) UNIQUE,
        EMPLOYEE_DEPARTMENT_ID BIGINT REFERENCES bl_3nf.ce_departments(DEPARTMENT_ID) ON DELETE CASCADE,
        EMPLOYEE_STATUS_ID BIGINT REFERENCES bl_3nf.ce_statuses(STATUS_ID) ON DELETE CASCADE,
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

    -- Customers SCD
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_customers_scd (
        CUSTOMER_ID BIGINT,
        CUSTOMER_SRC_ID VARCHAR(255),
        CUSTOMER_ZIP_CODE_ID BIGINT REFERENCES bl_3nf.ce_zip(ZIP_ID) ON DELETE CASCADE,
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

    -- Sales
    CREATE TABLE IF NOT EXISTS bl_3nf.ce_sales (
        GAME_NUMBER_ID BIGINT NOT NULL REFERENCES bl_3nf.ce_game_numbers(GAME_NUMBER_ID) ON DELETE CASCADE,
        CUSTOMER_ID BIGINT NOT NULL ,
        EMPLOYEE_ID BIGINT NOT NULL REFERENCES bl_3nf.ce_employees(EMPLOYEE_ID) ON DELETE CASCADE,
        RETAILER_LICENSE_NUMBER_ID BIGINT NOT NULL REFERENCES bl_3nf.ce_retailer_license_numbers(RETAILER_LICENSE_NUMBER_ID) ON DELETE CASCADE,
        PAYMENT_ID BIGINT NOT NULL REFERENCES bl_3nf.ce_payment_methods(PAYMENT_METHOD_ID) ON DELETE CASCADE,
        EVENT_DT DATE NOT NULL,
        TICKETS_BOUGHT INT CHECK (TICKETS_BOUGHT >= 0),
        PAYOUT FLOAT CHECK (PAYOUT >= 0),
        SALES FLOAT CHECK (SALES >= 0),
        TICKET_PRICE FLOAT CHECK (TICKET_PRICE >= 0),
        INSERT_DT DATE NOT NULL,
        UPDATE_DT DATE,
        CONSTRAINT pk_ce_sales PRIMARY KEY (
            GAME_NUMBER_ID,
            CUSTOMER_ID,
            EMPLOYEE_ID,
            RETAILER_LICENSE_NUMBER_ID,
            PAYMENT_ID,
            EVENT_DT
        )
    );

END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.create_bl_3nf_default_records()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO bl_3nf.ce_game_types VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_game_categories VALUES (-1, 'n. a.', -1,'n. a.', -1, -1, 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_game_numbers VALUES (-1, 'n. a.', -1, DATE '1900-01-01','n. a.', 'n. a.', -1, -1, -1, -1, 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_payment_methods VALUES (-1, 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_states VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_cities VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_zip VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_location_names VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_retailer_license_numbers VALUES (-1, 'n. a.', -1,'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_statuses VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_departments VALUES (-1, 'n. a.','n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_employees VALUES (-1, 'n. a.', -1, -1, DATE '1900-01-01', 'n. a.', 'n. a.', 'n. a.', -1, 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') on conflict do nothing;
    INSERT INTO bl_3nf.ce_customers_scd VALUES (-1, 'n. a.', -1, DATE '1900-01-01', 'n. a.', 'n. a.', DATE '1900-01-01', 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', TRUE, DATE '1900-01-01', DATE '1900-01-01', DATE '9999-12-31') on conflict do nothing;
END;
$$;




CREATE OR REPLACE PROCEDURE bl_cl.p_log_etl(
    p_procedure_name TEXT,
    p_rows_affected INT,
    p_log_message TEXT,
    p_log_level TEXT DEFAULT 'INFO'
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO bl_3nf.etl_logs (
        procedure_name,
        rows_affected,
        log_message,
        log_level
    )
    VALUES (
        p_procedure_name,
        p_rows_affected,
        p_log_message,
        UPPER(p_log_level)
    );
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Failed to log ETL event: %', SQLERRM;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.create_bl_dm_sequences()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create all surrogate key sequences
    CREATE SEQUENCE IF NOT EXISTS game_number_surr_seq START 1 INCREMENT 1;
    CREATE SEQUENCE IF NOT EXISTS customer_surr_seq START 1 INCREMENT 1;
    CREATE SEQUENCE IF NOT EXISTS retailer_license_number_surr_seq START 1 INCREMENT 1;
    CREATE SEQUENCE IF NOT EXISTS employee_surr_seq START 1 INCREMENT 1;
    CREATE SEQUENCE IF NOT EXISTS payment_method_surr_seq START 1 INCREMENT 1;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.create_bl_dm_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create schema
    CREATE SCHEMA IF NOT EXISTS bl_dm;

    -- Create tables
    CREATE TABLE IF NOT EXISTS bl_dm.dim_game_numbers (
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

    CREATE TABLE IF NOT EXISTS bl_dm.dim_retailer_license_numbers (
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

    CREATE TABLE IF NOT EXISTS bl_dm.dim_customers_scd (
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

    CREATE TABLE IF NOT EXISTS bl_dm.dim_employees (
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

    CREATE TABLE IF NOT EXISTS bl_dm.dim_payment_methods (
        payment_method_surr_id BIGINT PRIMARY KEY,
        payment_method_src_id VARCHAR(255) UNIQUE,
        payment_method_name VARCHAR(100),
        source_system VARCHAR(30),
        source_entity VARCHAR(30),
        insert_dt DATE,
        update_dt DATE
    );

    CREATE TABLE IF NOT EXISTS bl_dm.fct_sales (
    game_number_surr_id BIGINT REFERENCES bl_dm.dim_game_numbers(game_number_surr_id) ON DELETE CASCADE,
    customer_surr_id BIGINT REFERENCES bl_dm.dim_customers_scd(customer_surr_id) ON DELETE CASCADE,
    employee_surr_id BIGINT REFERENCES bl_dm.dim_employees(employee_surr_id) ON DELETE CASCADE,
    retailer_license_number_surr_id BIGINT REFERENCES bl_dm.dim_retailer_license_numbers(retailer_license_number_surr_id) ON DELETE CASCADE,
    payment_method_surr_id BIGINT REFERENCES bl_dm.dim_payment_methods(payment_method_surr_id) ON DELETE CASCADE,
    event_dt DATE REFERENCES bl_dm.dim_date(event_dt) ON DELETE CASCADE,
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


    RAISE NOTICE 'DM objects created successfully.';
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.create_bl_dm_default_records()
LANGUAGE plpgsql
AS $$
BEGIN
-- Insert default rows
    INSERT INTO bl_dm.dim_game_numbers VALUES 
        (-1, 'n. a.', 'n. a.', -1, 'n. a.', -1, 'n. a.', DATE '1900-01-01', 'n. a.', -1, -1, -1, -1, -1, -1, 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') 
        ON CONFLICT DO NOTHING;

    INSERT INTO bl_dm.dim_retailer_license_numbers VALUES 
        (-1, 'n. a.', 'n. a.', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') 
        ON CONFLICT DO NOTHING;

    INSERT INTO bl_dm.dim_customers_scd VALUES 
        (-1, 'n. a.', 'n. a.', DATE '1900-01-01', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.', 'n. a.', DATE '1900-01-01', 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01', DATE '9999-12-31', TRUE) 
        ON CONFLICT DO NOTHING;

    INSERT INTO bl_dm.dim_employees VALUES 
        (-1, 'n. a.', 'n. a.', DATE '1900-01-01', -1, 'n. a.', -1, 'n. a.', 'n. a.', 'n. a.', -1, 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') 
        ON CONFLICT DO NOTHING;

    INSERT INTO bl_dm.dim_payment_methods VALUES 
        (-1, 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', DATE '1900-01-01', DATE '1900-01-01') 
        ON CONFLICT DO NOTHING;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.create_p_log_etl_table()
LANGUAGE plpgsql
AS $$
BEGIN
   
   CREATE TABLE IF NOT EXISTS bl_3nf.etl_logs (
    log_id BIGSERIAL PRIMARY KEY,
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    procedure_name TEXT NOT NULL,
    rows_affected INT,
    log_message TEXT,
    log_level TEXT CHECK (log_level IN ('INFO', 'ERROR', 'WARN')) DEFAULT 'INFO'
);

END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.ensure_customer_scd_type_exists()
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema TEXT := 'bl_cl';
    v_type_name TEXT := 'customer_scd_type';
    v_full_type TEXT := format('%I.%I', v_schema, v_type_name);
BEGIN
    -- Check if the type already exists
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = v_type_name
          AND n.nspname = v_schema
    ) THEN
        EXECUTE format('
            CREATE TYPE %s AS (
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
                insert_dt DATE,
                start_dt DATE,
                end_dt DATE,
                is_active BOOLEAN
            )
        ', v_full_type);
    END IF;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.setup_all_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL bl_cl.create_bl_cl_objects();
    call bl_cl.create_p_log_etl_table();

    CALL bl_cl.create_bl_3nf_objects();
    CALL bl_cl.create_bl_3nf_default_records();
    CALL bl_cl.create_bl_3nf_sequences();

    CALL bl_cl.create_bl_dm_objects();
    CALL bl_cl.create_bl_dm_default_records();
    CALL bl_cl.create_bl_dm_sequences();
    call bl_cl.ensure_customer_scd_type_exists();

END;
$$;



CALL bl_cl.setup_all_objects();

