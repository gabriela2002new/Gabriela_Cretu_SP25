-- Drop source table if exists
DROP TABLE IF EXISTS sa_final_draw.src_final_draw CASCADE;

-- Drop foreign table if exists
DROP FOREIGN TABLE IF EXISTS sa_final_draw.ext_final_draw CASCADE;

-- Drop server if exists (optional)
DROP SERVER IF EXISTS file_server CASCADE;

-- Create server
CREATE SERVER file_server FOREIGN DATA WRAPPER file_fdw;

-- Create external foreign table for Final_Draw aligned to FCT_SALES and dimensions
CREATE FOREIGN TABLE sa_final_draw.ext_final_draw (
    -- Sales Information
    transaction_dt_id INT,                 -- mapped from date_id
    retailer_license_number INT,
    customer_id VARCHAR(255),
    employee_id VARCHAR(255),
    game_number VARCHAR(255),                      -- mapped from GAME_NUMBER
    payment_method_id INT,                -- mapped from PAYMENT_METHOD_ID
    sales INT,
    tickets_bought INT,
    payout INT,

    -- Customer Information
    customer_email VARCHAR(255),
    customer_phone VARCHAR(255),
    customer_registration_dt_id DATE,
    customer_state VARCHAR(255),
    customer_city VARCHAR(255),
    customer_zip_code INT,                -- mapped from customer_zip

    -- Employee Information
    employee_email VARCHAR(255),
    employee_phone VARCHAR(255),
    employee_hire_dt_id DATE,
    employee_salary INT,

    -- Retailer Information
    retailer_location_name VARCHAR(255),
    retailer_location_zip_code TEXT,
    retailer_location_state VARCHAR(255),

    -- Game Information
    game_category VARCHAR(255),
    game_type VARCHAR(255),
    ticket_price INT,
    winning_chance FLOAT,
    winning_jackpot INT,
    draw_dt_id DATE,

    -- Payment Information
    payment_method_name VARCHAR(255),    -- mapped from PAYMENT_METHOD_NAME
    is_allowed_in_store BOOLEAN,
    is_allowed_online BOOLEAN,
    is_cash_equivalent BOOLEAN,
    is_digital BOOLEAN,
    credit_based BOOLEAN,
    status VARCHAR(255),
    notes VARCHAR(255)
)
SERVER file_server
OPTIONS (
    filename 'C:/Users/acer/AppData/Roaming/DBeaverData/workspace6/General/Final_Draw.csv',
    format 'csv',
    header 'true'
);



-- Create empty source table with same structure but no data
CREATE TABLE sa_final_draw.src_final_draw AS
SELECT DISTINCT * FROM sa_final_draw.ext_final_draw WHERE false;




select *
from sa_final_draw.src_final_draw sfd ;

-- Create schema for 3NF if not exists
CREATE SCHEMA IF NOT EXISTS bl_3nf;

-- Drop retailers table if exists (to reset)
DROP TABLE IF EXISTS bl_3nf.retailers CASCADE;

-- Create retailers table (3NF)
CREATE TABLE bl_3nf.retailers (
    retailer_license_number VARCHAR(255) PRIMARY KEY,
    retailer_location_name VARCHAR(255),
    retailer_location_zip_code VARCHAR(255),
    retailer_location_state VARCHAR(255)
);

INSERT INTO bl_3nf.retailers (retailer_license_number, retailer_location_name, retailer_location_zip_code, retailer_location_state)
SELECT DISTINCT
    retailer_license_number,
    retailer_location_name,
    retailer_location_zip_code,
    retailer_location_state
FROM sa_final_draw.src_final_draw;

select *
from bl_3nf.retailers;



