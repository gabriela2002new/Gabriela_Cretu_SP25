-- Drop source table if exists
DROP TABLE IF EXISTS sa_final_scratch.src_final_scratch CASCADE;

-- Drop foreign table if exists
DROP FOREIGN TABLE IF EXISTS sa_final_scratch.ext_final_scratch CASCADE;

-- Drop server if exists (optional)
DROP SERVER IF EXISTS file_server CASCADE;

-- Create server
CREATE SERVER file_server FOREIGN DATA WRAPPER file_fdw;

-- Create external foreign table for Final_Draw aligned to FCT_SALES and dimensions

CREATE FOREIGN TABLE sa_final_scratch.ext_final_scratch (
    -- Sales Information
    transaction_dt_id INT,
    retailer_license_number INT,
    customer_id VARCHAR(255),
    employee_id VARCHAR(255),
    game_number VARCHAR(255),
    tickets_bought INT,
    payment_method_id INT,
    sales INT,
    payout INT,

    -- Customer Information
    customer_name TEXT,
    customer_gender CHAR(1),
    customer_dob DATE,

    -- Employee Information
    employee_name VARCHAR(255),
    employee_department VARCHAR(255),
    employee_status VARCHAR(255),

    -- Retailer Information
    retailer_location_name VARCHAR(50),
    retailer_location_zip_code TEXT,
    retailer_location_city VARCHAR(100),

    -- Game Information
    ticket_price INT,
    game_category VARCHAR(100),
    game_type VARCHAR(50),
    average_odds VARCHAR(10),
    average_odds_prob FLOAT,
    top_prize INT,
    mid_prize INT,
    small_prize INT,

    -- Payment Information
    payment_method_name VARCHAR(100),
    is_allowed_in_store BOOLEAN,
    is_allowed_online BOOLEAN,
    is_cash_equivalent BOOLEAN,
    is_digital BOOLEAN,
    credit_based BOOLEAN,
    status VARCHAR(20),
    notes TEXT
)
SERVER file_server
OPTIONS (
    filename 'C:/Users/acer/AppData/Roaming/DBeaverData/workspace6/General/Final_Scratch.csv',
    format 'csv',
    header 'true'
);



-- Create empty source table with same structure but no data
CREATE TABLE sa_final_scratch.src_final_scratch AS
SELECT DISTINCT * FROM sa_final_scratch.ext_final_scratch WHERE false;




select *
from sa_final_scratch.src_final_scratch sfd ;



