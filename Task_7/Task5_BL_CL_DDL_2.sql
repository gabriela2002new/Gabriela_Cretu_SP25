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

drop table if exists bl_cl.mta_game_types;


CREATE TABLE bl_cl.wrk_game_types (
    game_type_src_id   VARCHAR,
    game_type_name     VARCHAR,
    source_system      VARCHAR,
    source_entity      VARCHAR,
    load_dt            DATE
);

CREATE TABLE bl_cl.lkp_game_types (
    game_type_id       INT ,
    game_type_src_id   VARCHAR,
    game_type_name     VARCHAR,
    source_system      VARCHAR,
    source_entity      VARCHAR,
    insert_dt          DATE,
    update_dt          DATE,
    CONSTRAINT uq_game_type UNIQUE (game_type_src_id, source_system, source_entity)
);

drop table if exists bl_cl.lkp_game_types;
CREATE SEQUENCE IF NOT EXISTS bl_cl.t_map_game_types_seq START WITH 1 INCREMENT BY 1;
-- ERROR: syntax error at or near "IF"

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


DROP TABLE IF EXISTS bl_cl.wrk_game_categories;

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

DROP TABLE IF EXISTS bl_cl.mta_game_numbers;

CREATE table if not exists bl_cl.mta_game_numbers (
    column_name       VARCHAR(100) NOT NULL,
    source_column     VARCHAR(100) NOT NULL,
    data_type         VARCHAR(50) NOT NULL,
    transformation    VARCHAR(255) NOT NULL,
    nullable          BOOLEAN NOT NULL,
    description       VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS bl_cl.wrk_game_numbers;

-- Working (staging) table to hold source data before mapping
CREATE TABLE bl_cl.wrk_game_numbers (
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
    insert_dt DATE NOT NULL
);

DROP TABLE IF EXISTS bl_cl.lkp_game_numbers;

-- Lookup (final mapping) table with surrogate keys and unique constraint
CREATE TABLE bl_cl.lkp_game_numbers (
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

DROP TABLE IF EXISTS bl_cl.mta_payment_methods;

CREATE table if not exists bl_cl.mta_payment_methods (
    column_name      VARCHAR(100) NOT NULL,
    source_column    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation   VARCHAR(255) NOT NULL,
    nullable         BOOLEAN      NOT NULL,
    description      VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS bl_cl.wrk_payment_methods;

CREATE TABLE bl_cl.wrk_payment_methods (
    payment_method_src_id   VARCHAR(255) NOT NULL,
    payment_method_name     VARCHAR(100),
    source_system           VARCHAR(30) NOT NULL,
    source_entity           VARCHAR(30) NOT NULL,
    insert_dt               DATE NOT NULL
);

drop table if exists bl_cl.lkp_payment_methods;
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
CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_payment_methods_seq START 1;


-- ======================================
-- States
-- ======================================

DROP TABLE IF EXISTS bl_cl.mta_states;

CREATE TABLE bl_cl.mta_states (
    column_name      VARCHAR(100) NOT NULL,
    source_column    VARCHAR(100) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    transformation   VARCHAR(255) NOT NULL,
    nullable         BOOLEAN      NOT NULL,
    description      VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS bl_cl.wrk_states;

CREATE TABLE bl_cl.wrk_states (
    state_src_id   VARCHAR(255) NOT NULL,
    source_system  VARCHAR(30) NOT NULL,
    source_entity  VARCHAR(30) NOT NULL,
    insert_dt      DATE NOT NULL
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

CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_states_seq START 1;


-- ======================================
-- Cities
-- ======================================

DROP TABLE IF EXISTS bl_cl.mta_cities;

CREATE TABLE bl_cl.mta_cities (
    column_name     VARCHAR(100) NOT NULL,
    source_column   VARCHAR(100) NOT NULL,
    data_type       VARCHAR(50)  NOT NULL,
    transformation  VARCHAR(255) NOT NULL,
    nullable        BOOLEAN      NOT NULL,
    description     VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS bl_cl.wrk_cities;

CREATE TABLE bl_cl.wrk_cities (
    city_src_id     VARCHAR(255) NOT NULL,
    state_name      VARCHAR(100),
    source_system   VARCHAR(30) NOT NULL,
    source_entity   VARCHAR(30) NOT NULL,
    insert_dt       DATE NOT NULL
);

drop table if exists bl_cl.lkp_cities;
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

CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_cities_seq START 1;


-- ======================================
-- Zipcodes
-- ======================================


DROP TABLE IF EXISTS bl_cl.mta_zips;

CREATE TABLE bl_cl.mta_zips (
    column_name     VARCHAR(100) NOT NULL,
    source_column   VARCHAR(100) NOT NULL,
    data_type       VARCHAR(50)  NOT NULL,
    transformation  VARCHAR(255) NOT NULL,
    nullable        BOOLEAN      NOT NULL,
    description     VARCHAR(255) NOT NULL
);


DROP TABLE IF EXISTS bl_cl.wrk_zips;

CREATE TABLE bl_cl.wrk_zips (
    zip_src_id     VARCHAR(20) NOT NULL,
    city_name      VARCHAR(100),
    source_system  VARCHAR(30) NOT NULL,
    source_entity  VARCHAR(30) NOT NULL,
    insert_dt      DATE NOT NULL
);

DROP TABLE IF EXISTS bl_cl.lkp_zips;

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

CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_zips_seq START 1;


-- ======================================
-- Location Names
-- ======================================

DROP TABLE IF EXISTS bl_cl.mta_location_names;

CREATE table if not exists bl_cl.mta_location_names (
    column_name     VARCHAR(100) NOT NULL,
    source_column   VARCHAR(100) NOT NULL,
    data_type       VARCHAR(50)  NOT NULL,
    transformation  VARCHAR(255) NOT NULL,
    nullable        BOOLEAN      NOT NULL,
    description     VARCHAR(255) NOT NULL
);


DROP TABLE IF EXISTS bl_cl.wrk_location_names;

CREATE TABLE bl_cl.wrk_location_names (
    location_name_src_id VARCHAR(255) NOT NULL,
    zip_name           VARCHAR(20)  NOT NULL,
    source_system        VARCHAR(30)  NOT NULL,
    source_entity        VARCHAR(30)  NOT NULL,
    insert_dt            DATE         NOT NULL
);

DROP TABLE IF EXISTS bl_cl.lkp_location_names;

CREATE TABLE bl_cl.lkp_location_names (
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

CREATE SEQUENCE IF NOT EXISTS bl_cl.ce_location_names_seq START 1;

-- ======================================
-- Retailers
-- ======================================

DROP TABLE IF EXISTS bl_cl.mta_retailers;

CREATE TABLE bl_cl.mta_retailers (
    column_name     VARCHAR(100) NOT NULL,
    source_column   VARCHAR(100) NOT NULL,
    data_type       VARCHAR(50)  NOT NULL,
    transformation  VARCHAR(255) NOT NULL,
    nullable        BOOLEAN      NOT NULL,
    description     VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS bl_cl.wrk_retailers;

CREATE TABLE bl_cl.wrk_retailers (
    retailer_license_number_src_id  VARCHAR(100) NOT NULL,
    location_name                   VARCHAR(255),
    source_system                   VARCHAR(30) NOT NULL,
    source_entity                   VARCHAR(30) NOT NULL,
    insert_dt                       DATE NOT NULL
);


DROP TABLE IF EXISTS bl_cl.lkp_retailers;

CREATE TABLE bl_cl.lkp_retailers (
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

CREATE SEQUENCE IF NOT EXISTS bl_cl.t_mapping_retailer_license_seq START WITH 1;




-- ======================================
-- Statuses
-- ======================================
DROP TABLE IF EXISTS bl_cl.mta_statuses;

CREATE TABLE bl_cl.mta_statuses (
    column_name     VARCHAR(100) NOT NULL,
    source_column   VARCHAR(100) NOT NULL,
    data_type       VARCHAR(50)  NOT NULL,
    transformation  VARCHAR(255) NOT NULL,
    nullable        BOOLEAN      NOT NULL,
    description     VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS bl_cl.wrk_statuses;

CREATE TABLE bl_cl.wrk_statuses (
    status_src_id VARCHAR(100) NOT NULL,
    source_system VARCHAR(30) NOT NULL,
    source_entity VARCHAR(30) NOT NULL,
    insert_dt     DATE NOT NULL
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

CREATE SEQUENCE IF NOT EXISTS bl_cl.t_mapping_status_seq START WITH 1;


-- ======================================
-- Departments
-- ======================================

DROP TABLE IF EXISTS bl_cl.mta_departments;

CREATE table if not exists bl_cl.mta_departments (
    column_name     VARCHAR(100) NOT NULL,
    source_column   VARCHAR(100) NOT NULL,
    data_type       VARCHAR(50)  NOT NULL,
    transformation  VARCHAR(255) NOT NULL,
    nullable        BOOLEAN      NOT NULL,
    description     VARCHAR(255) NOT NULL
);



DROP TABLE IF EXISTS bl_cl.wrk_departments;

CREATE TABLE bl_cl.wrk_departments (
    department_src_id VARCHAR(100) NOT NULL,
    source_system VARCHAR(30) NOT NULL,
    source_entity VARCHAR(30) NOT NULL,
    insert_dt DATE NOT NULL
);

drop table if exists bl_cl.lkp_departments;

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

CREATE SEQUENCE IF NOT EXISTS bl_cl.t_mapping_department_seq START WITH 1 INCREMENT BY 1;


-- ======================================
-- Employees
-- ======================================

CREATE TABLE IF NOT EXISTS bl_cl.mta_employees (
    column_name     VARCHAR(100) NOT NULL,
    source_column   VARCHAR(100) NOT NULL,
    data_type       VARCHAR(50)  NOT NULL,
    transformation  VARCHAR(255) NOT NULL,
    nullable        BOOLEAN      NOT NULL,
    description     VARCHAR(255) NOT NULL
);


DROP TABLE IF EXISTS bl_cl.wrk_employees;

CREATE TABLE bl_cl.wrk_employees (
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
    insert_dt DATE NOT NULL
);

drop table if exists bl_cl.lkp_employees;
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

CREATE SEQUENCE IF NOT EXISTS bl_cl.t_mapping_employees_seq START WITH 1 INCREMENT BY 1;

-- ======================================
-- Customers
-- ======================================

drop table if exists bl_cl.metadata_customers 
CREATE TABLE IF NOT EXISTS bl_cl.mta_customers (
    column_name           VARCHAR,     -- Target column in t_mapping_customers
    source_column_name    VARCHAR,     -- Column name from source (if applicable)
    data_type             VARCHAR,     -- Data type used in target
    transformation_rule   TEXT,        -- Logic to transform or derive the value
    is_nullable           BOOLEAN,     -- Whether null is allowed in the target
    notes                 TEXT         -- Optional explanation or comments
);


drop table if exists bl_cl.wrk_customers;
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

drop table if exists bl_cl.lkp_customers;
CREATE TABLE IF NOT EXISTS bl_cl.lkp_customers (
    customer_id              BIGINT ,
    customer_src_id          VARCHAR(100) NOT NULL,
    zip_name                 VARCHAR(20) DEFAULT 'n.a.',
    customer_registration_dt DATE NOT NULL DEFAULT '1900-01-01',
    customer_name            VARCHAR(255) DEFAULT 'n.a.',
    customer_gender          VARCHAR(10) DEFAULT 'n.a.',
    customer_dob             DATE NOT NULL DEFAULT '1900-01-01',
    customer_email           VARCHAR(255) DEFAULT 'n.a.',
    customer_phone           VARCHAR(50) DEFAULT 'n.a.',
    source_system            VARCHAR(100) NOT NULL,
    source_entity            VARCHAR(100) NOT NULL,
    is_active                BOOLEAN NOT NULL DEFAULT TRUE,
    insert_dt                DATE NOT NULL DEFAULT CURRENT_DATE,
    start_dt                 DATE NOT NULL DEFAULT CURRENT_DATE,
    end_dt                   DATE NOT NULL DEFAULT DATE '9999-12-31',

    CONSTRAINT uq_lkp_customers UNIQUE (
        customer_src_id, source_system, source_entity, start_dt
    )
);

CREATE SEQUENCE IF NOT EXISTS bl_cl.seq_lkp_customers
    START WITH 1
    INCREMENT BY 1;
   
   
   
   
DROP TABLE IF EXISTS bl_cl.mta_sales;

-- ============================================
-- Sales
-- ============================================
 
CREATE TABLE IF NOT EXISTS bl_cl.mta_sales (
    column_name          VARCHAR,
    source_column_name   VARCHAR,
    data_type            VARCHAR,
    transformation_rule  TEXT,
    is_nullable          BOOLEAN,
    notes                TEXT
);


DROP TABLE IF EXISTS bl_cl.wrk_sales;

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


DROP TABLE IF EXISTS bl_cl.lkp_sales;

CREATE TABLE bl_cl.lkp_sales (
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

