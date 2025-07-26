INSERT INTO sa_final_scratch.src_final_scratch (
    transaction_dt,
    customer_id,
    retailer_license_number,
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
) VALUES (
    '2021-02-15',                        -- transaction_dt
    'EDNA_036',                             -- new customer_id
    'EL PASO_004',                      -- new retailer_license_number
    'EMP_100099_1',                    -- new employee_id
    'Scratch_100099-65432-9876',       -- new game_number
    4,                                 -- tickets_bought
    2,                                 -- payment_method_id (different)
    20.00,                             -- sales
    5.00,                              -- payout
    'Jason Lee',                       -- new customer_name
    'M',                               -- customer_gender
    '1985-09-17',                      -- customer_dob
    'Tina Romero',                     -- new employee_name
    'Sales Associate',                 -- employee_department
    'Active',                          -- employee_status
    'Quick Stop Market',               -- retailer_location_name
    '79936',                           -- retailer_location_zip_code
    'El Paso',                         -- retailer_location_city
    5.00,                              -- ticket_price
    'Scratch Tickets',                 -- game_category
    'Scratch',                         -- game_type
    '1:3.75',                          -- average_odds
    0.26667,                           -- average_odds_prob
    75000,                             -- top_prize
    3000,                              -- mid_prize
    100,                               -- small_prize
    'Credit Card'                      -- new payment_method_name
);



select * from sa_final_scratch.src_final_scratch sfs ;


INSERT INTO sa_final_scratch.src_final_scratch (
    transaction_dt,
    customer_id,
    retailer_license_number,
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
) VALUES (
    '2024-02-15',                        -- transaction_dt
    'EDNA_036',                             -- new customer_id
    'EL PASO_004',                      -- new retailer_license_number
    'EMP_100099_1',                    -- new employee_id
    'Scratch_100099-65432-9876',       -- new game_number
    4,                                 -- tickets_bought
    2,                                 -- payment_method_id (different)
    20.00,                             -- sales
    5.00,                              -- payout
    'Jason Lee',                       -- new customer_name
    'M',                               -- customer_gender
    '1985-09-17',                      -- customer_dob
    'Tina Romero',                     -- new employee_name
    'Sales Associate',                 -- employee_department
    'Active',                          -- employee_status
    'Quick Stop Market',               -- retailer_location_name
    '79936',                           -- retailer_location_zip_code
    'El Paso',                         -- retailer_location_city
    5.00,                              -- ticket_price
    'Scratch Tickets',                 -- game_category
    'Scratch',                         -- game_type
    '1:3.75',                          -- average_odds
    0.26667,                           -- average_odds_prob
    75000,                             -- top_prize
    3000,                              -- mid_prize
    100,                               -- small_prize
    'Credit Card'                      -- new payment_method_name
);
