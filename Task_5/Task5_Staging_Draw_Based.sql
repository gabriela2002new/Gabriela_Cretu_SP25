-- Create server
CREATE server if not exists file_server FOREIGN DATA WRAPPER file_fdw;

-- Create external foreign table for Final_Draw aligned to FCT_SALES and dimensions
create FOREIGN table if not exists sa_final_draw.ext_final_draw (
    -- Sales Information
    transaction_dt_id VARCHAR(255),                 -- mapped from date_id
    retailer_license_number VARCHAR(255),
    customer_id VARCHAR(255),
    employee_id VARCHAR(255),
    game_number VARCHAR(255),                      -- mapped from GAME_NUMBER
    payment_method_id VARCHAR(255),                -- mapped from PAYMENT_METHOD_ID
    sales VARCHAR(255),
    tickets_bought VARCHAR(255),
    payout VARCHAR(255),

    -- Customer Information
    customer_email VARCHAR(255),
    customer_phone VARCHAR(255),
    customer_registration_dt_id VARCHAR(255),
    customer_state VARCHAR(255),
    customer_city VARCHAR(255),
    customer_zip_code VARCHAR(255),                -- mapped from customer_zip

    -- Employee Information
    employee_email VARCHAR(255),
    employee_phone VARCHAR(255),
    employee_hire_dt_id VARCHAR(255),
    employee_salary VARCHAR(255),

    -- Retailer Information
    retailer_location_name VARCHAR(255),
    retailer_location_zip_code VARCHAR(255),
    retailer_location_state VARCHAR(255),

    -- Game Information
    game_category VARCHAR(255),
    game_type VARCHAR(255),
    ticket_price VARCHAR(255),
    winning_chance VARCHAR(255),
    winning_jackpot VARCHAR(255),
    draw_dt_id VARCHAR(255),

    -- Payment Information
    payment_method_name VARCHAR(255)   -- mapped from PAYMENT_METHOD_NAME
   
)
SERVER file_server
OPTIONS (
    filename 'C:\csv\Final_Draw_fixed_lowercase.csv',
    format 'csv',
    header 'true'
);



CREATE TABLE IF NOT EXISTS sa_final_draw.src_final_draw (
    -- Sales Information
    transaction_dt_id VARCHAR(255),
    retailer_license_number VARCHAR(255),
    customer_id VARCHAR(255),
    employee_id VARCHAR(255),
    game_number VARCHAR(255),
    payment_method_id VARCHAR(255),
    sales VARCHAR(255),
    tickets_bought VARCHAR(255),
    payout VARCHAR(255),

    -- Customer Information
    customer_email VARCHAR(255),
    customer_phone VARCHAR(255),
    customer_registration_dt_id VARCHAR(255),
    customer_state VARCHAR(255),
    customer_city VARCHAR(255),
    customer_zip_code VARCHAR(255),

    -- Employee Information
    employee_email VARCHAR(255),
    employee_phone VARCHAR(255),
    employee_hire_dt_id VARCHAR(255),
    employee_salary VARCHAR(255),

    -- Retailer Information
    retailer_location_name VARCHAR(255),
    retailer_location_zip_code VARCHAR(255),
    retailer_location_state VARCHAR(255),

    -- Game Information
    game_category VARCHAR(255),
    game_type VARCHAR(255),
    ticket_price VARCHAR(255),
    winning_chance VARCHAR(255),
    winning_jackpot VARCHAR(255),
    draw_dt_id VARCHAR(255),

    -- Payment Information
    payment_method_name VARCHAR(255)
);

INSERT INTO sa_final_draw.src_final_draw (
    transaction_dt_id,
    retailer_license_number,
    customer_id,
    employee_id,
    game_number,
    payment_method_id,
    sales,
    tickets_bought,
    payout,
    customer_email,
    customer_phone,
    customer_registration_dt_id,
    customer_state,
    customer_city,
    customer_zip_code,
    employee_email,
    employee_phone,
    employee_hire_dt_id,
    employee_salary,
    retailer_location_name,
    retailer_location_zip_code,
    retailer_location_state,
    game_category,
    game_type,
    ticket_price,
    winning_chance,
    winning_jackpot,
    draw_dt_id,
    payment_method_name
)
SELECT
    transaction_dt_id,
    retailer_license_number,
    customer_id,
    employee_id,
    game_number,
    payment_method_id,
    sales,
    tickets_bought,
    payout,
    customer_email,
    customer_phone,
    customer_registration_dt_id,
    customer_state,
    customer_city,
    customer_zip_code,
    employee_email,
    employee_phone,
    employee_hire_dt_id,
    employee_salary,
    retailer_location_name,
    retailer_location_zip_code,
    retailer_location_state,
    game_category,
    game_type,
    ticket_price,
    winning_chance,
    winning_jackpot,
    draw_dt_id,
    payment_method_name
FROM sa_final_draw.ext_final_draw;


select transaction_dt_id,
    retailer_license_number,
    customer_id,
    employee_id,
    game_number,
    tickets_bought,
    payment_method_id,
    sales,
    payout,
    ticket_price
from sa_final_draw.src_final_draw sfd ;




