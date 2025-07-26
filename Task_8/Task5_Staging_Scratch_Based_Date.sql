DROP FOREIGN TABLE IF EXISTS sa_final_scratch.ext_final_scratch;
DROP TABLE IF EXISTS sa_final_scratch.src_final_scratch;
DROP SERVER IF EXISTS file_server CASCADE;

-- Create server
CREATE server if not exists file_server FOREIGN DATA WRAPPER file_fdw;

-- Create external foreign table for Final_Draw aligned to FCT_SALES and dimensions

CREATE FOREIGN table if not exists sa_final_scratch.ext_final_scratch (
    -- Sales Information
    transaction_dt VARCHAR(255),
    retailer_license_number VARCHAR(255),
    customer_id VARCHAR(255),
    employee_id VARCHAR(255),
    game_number VARCHAR(255),
    tickets_bought VARCHAR(255),
    payment_method_id VARCHAR(255),
    sales VARCHAR(255),
    payout VARCHAR(255),

    -- Customer Information
    customer_name VARCHAR(255),
    customer_gender VARCHAR(255),
    customer_dob VARCHAR(255),

    -- Employee Information
    employee_name VARCHAR(255),
    employee_department VARCHAR(255),
    employee_status VARCHAR(255),

    -- Retailer Information
    retailer_location_name VARCHAR(255),
    retailer_location_zip_code VARCHAR(255),
    retailer_location_city VARCHAR(255),

    -- Game Information
    ticket_price VARCHAR(255),
    game_category VARCHAR(255),
    game_type VARCHAR(255),
    average_odds VARCHAR(255),
    average_odds_prob VARCHAR(255),
    top_prize VARCHAR(255),
    mid_prize VARCHAR(255),
    small_prize VARCHAR(255),

    -- Payment Information
    payment_method_name VARCHAR(255)
)
SERVER file_server
OPTIONS (
    filename 'C:\csv\Final_Scratch_with_dates.csv',
    format 'csv',
    header 'true'
);



CREATE TABLE IF NOT EXISTS sa_final_scratch.src_final_scratch (
    -- Sales Information
    transaction_dt VARCHAR(255),
    retailer_license_number VARCHAR(255),
    customer_id VARCHAR(255),
    employee_id VARCHAR(255),
    game_number VARCHAR(255),
    tickets_bought VARCHAR(255),
    payment_method_id VARCHAR(255),
    sales VARCHAR(255),
    payout VARCHAR(255),

    -- Customer Information
    customer_name VARCHAR(255),
    customer_gender VARCHAR(255),
    customer_dob VARCHAR(255),

    -- Employee Information
    employee_name VARCHAR(255),
    employee_department VARCHAR(255),
    employee_status VARCHAR(255),

    -- Retailer Information
    retailer_location_name VARCHAR(255),
    retailer_location_zip_code VARCHAR(255),
    retailer_location_city VARCHAR(255),

    -- Game Information
    ticket_price VARCHAR(255),
    game_category VARCHAR(255),
    game_type VARCHAR(255),
    average_odds VARCHAR(255),
    average_odds_prob VARCHAR(255),
    top_prize VARCHAR(255),
    mid_prize VARCHAR(255),
    small_prize VARCHAR(255),

    -- Payment Information
    payment_method_name VARCHAR(255)
);

INSERT INTO sa_final_scratch.src_final_scratch (
    transaction_dt,
    retailer_license_number,
    customer_id,
    employee_id,
    game_number,
    tickets_bought,
    payment_method_id,
    sales,
    payout,
    customer_name,
    customer_gender,
    customer_dob,
    employee_name,
    employee_department,
    employee_status,
    retailer_location_name,
    retailer_location_zip_code,
    retailer_location_city,
    ticket_price,
    game_category,
    game_type,
    average_odds,
    average_odds_prob,
    top_prize,
    mid_prize,
    small_prize,
    payment_method_name
)
SELECT
    transaction_dt,
    retailer_license_number,
    customer_id,
    employee_id,
    game_number,
    tickets_bought,
    payment_method_id,
    sales,
    payout,
    customer_name,
    customer_gender,
    customer_dob,
    employee_name,
    employee_department,
    employee_status,
    retailer_location_name,
    retailer_location_zip_code,
    retailer_location_city,
    ticket_price,
    game_category,
    game_type,
    average_odds,
    average_odds_prob,
    top_prize,
    mid_prize,
    small_prize,
    payment_method_name
FROM sa_final_scratch.ext_final_scratch;


select transaction_dt,
    retailer_license_number,
    customer_id,
    employee_id,
    game_number,
    tickets_bought,
    payment_method_id,
    sales,
    payout,
    ticket_price
from sa_final_scratch.src_final_scratch sfd ;



