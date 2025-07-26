----Test that the procedure can be executed repeatedly with consistent results--------------------
--------------------------------------------------------------------------------------------------------------------------
---1.1.Run the procedure:
-----------------------------------------------------------------------------------------------
CALL bl_cl.sp_run_batch_etl_by_day(DATE '2022-02-06', DATE '2022-02-07');


----------------------------------------------------------------------------------------------------------------------------------------
---1.2.Query the logging table for affected rows:
-------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM bl_3nf.etl_logs
WHERE procedure_name IN (
    'sp_upsert_lkp_game_types',
    'sp_upsert_lkp_game_categories',
    'sp_upsert_lkp_game_numbers',
    'sp_upsert_lkp_payment_methods',
    'sp_upsert_lkp_states',
    'sp_upsert_lkp_cities',
    'sp_upsert_lkp_zips',
    'sp_upsert_lkp_location_names',
    'sp_upsert_lkp_retailers',
    'sp_upsert_lkp_statuses',
    'sp_upsert_lkp_departments',
    'sp_upsert_lkp_employees',
    'sp_upsert_lkp_customers',
    'sp_upsert_lkp_sales',
    
    'p_load_ce_game_types',
    'p_load_ce_game_categories',
    'p_load_ce_game_numbers',
    'p_load_ce_payment_methods',
    'p_load_ce_states',
    'p_load_ce_cities',
    'p_load_ce_zip',
    'p_load_ce_location_names',
    'p_load_retailer_license_numbers',
    'p_load_ce_statuses',
    'p_load_ce_departments',
    'p_load_ce_employees',
    'p_load_ce_customers_scd',
    'p_load_ce_sales',
    
    'sp_upsert_dim_game_numbers',
    'sp_upsert_dim_payment_methods',
    'sp_upsert_dim_retailer_license_numbers',
    'sp_upsert_dim_employees',
    'sp_upsert_dim_customers_scd',
    'sp_insert_fct_sales'
)
order by procedure_name, log_id ;




----------------------------------------------------------------------------------------------------------------------------------------
---1.3.Take a screenshot of the logging result
-----------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------
---1.4. Run the procedure again with the same input
--------------------------------------------------------------------------------------------------------------------------------------------------
CALL bl_cl.sp_run_batch_etl_by_day(DATE '2022-02-06', DATE '2022-02-07');

----------------------------------------------------------------------------------------------------------------------------------------
---1.5. Query the login table again
--------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM bl_3nf.etl_logs
WHERE procedure_name IN (
    'sp_upsert_lkp_game_types',
    'sp_upsert_lkp_game_categories',
    'sp_upsert_lkp_game_numbers',
    'sp_upsert_lkp_payment_methods',
    'sp_upsert_lkp_states',
    'sp_upsert_lkp_cities',
    'sp_upsert_lkp_zips',
    'sp_upsert_lkp_location_names',
    'sp_upsert_lkp_retailers',
    'sp_upsert_lkp_statuses',
    'sp_upsert_lkp_departments',
    'sp_upsert_lkp_employees',
    'sp_upsert_lkp_customers',
    'sp_upsert_lkp_sales',
    
    'p_load_ce_game_types',
    'p_load_ce_game_categories',
    'p_load_ce_game_numbers',
    'p_load_ce_payment_methods',
    'p_load_ce_states',
    'p_load_ce_cities',
    'p_load_ce_zip',
    'p_load_ce_location_names',
    'p_load_retailer_license_numbers',
    'p_load_ce_statuses',
    'p_load_ce_departments',
    'p_load_ce_employees',
    'p_load_ce_customers_scd',
    'p_load_ce_sales',
    
    'sp_upsert_dim_game_numbers',
    'sp_upsert_dim_payment_methods',
    'sp_upsert_dim_retailer_license_numbers',
    'sp_upsert_dim_employees',
    'sp_upsert_dim_customers_scd',
    'sp_insert_fct_sales'
)
order by procedure_name, log_id ;

----------------------------------------------------------------------------------------------------------------------------------------
---1.6. Confirm number of rows affected is 0 (or explain why)
----------------------------------------------------------------------------------------------------------------------------------------


----Test that the SCD2 procedure works correctly--------------------

--------------------------------------------------------------------------------------------------------------------------
---2.1.Take screenshot of original CSV data (some rows)
-----------------------------------------------------------------------------------------------
SELECT *
FROM sa_final_draw.src_final_draw sfd
WHERE sfd.transaction_dt::DATE BETWEEN DATE '2022-02-06' AND DATE '2022-02-07' and sfd.customer_id ='EDNA_036';

SELECT *
FROM sa_final_scratch.src_final_scratch sfd
WHERE sfd.transaction_dt::DATE BETWEEN DATE '2022-02-06' AND DATE '2022-02-07'and sfd.customer_id ='EDNA_036';

select * from bl_cl.lkp_customers lc where customer_src_id ='EDNA_036' order by lc.customer_id desc;

select * from bl_3nf.ce_customers_scd ccs where customer_src_id ='EDNA_036' order by ccs.customer_id desc ;

select * from bl_dm.dim_customers_scd dcs where dcs.customer_src_id ='1526' order by dcs.customer_src_id desc ;


----------------------------------------------------------------------------------------------------------------------------------------
---2.2.Take screenshot of SCD2 data in 3NF and DM layers
-------------------------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_src_id IN ('EDNA_036', 'ANOTHER_ID') ORDER BY customer_id;
SELECT * FROM bl_dm.dim_customers_scd WHERE customer_src_id IN ('EDNA_036', 'ANOTHER_ID') ORDER BY customer_surr_id;



----------------------------------------------------------------------------------------------------------------------------------------
---2.3 Prepare an additional CSV with changes
-----------------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO sa_final_scratch.src_final_scratch (
  transaction_dt, customer_id, retailer_license_number, employee_id, game_number, tickets_bought,
  payment_method_id, sales, payout, customer_name, customer_gender, customer_dob,
  employee_name, employee_department, employee_status, retailer_location_name, retailer_location_zip_code,
  retailer_location_city, ticket_price, game_category, game_type, average_odds, average_odds_prob,
  top_prize, mid_prize, small_prize, payment_method_name
) VALUES (
  '2024-02-15', 'EDNA_036', 'EL PASO_004', 'EMP_100099_1', 'Scratch_100099-65432-9876', 4,
  2, 20.00, 5.00, 'Jason Lee', 'M', '1985-09-17',
  'Tina Romero', 'Sales Associate', 'Active', 'Quick Stop Market', '79936',
  'El Paso', 5.00, 'Scratch Tickets', 'Scratch', '1:3.75', 0.26667,
  75000, 3000, 100, 'Credit Card'
);

SELECT *
FROM sa_final_scratch.src_final_scratch sfd
WHERE sfd.transaction_dt::DATE BETWEEN DATE '2024-02-15' AND DATE '2024-02-15'and sfd.customer_id ='EDNA_036';
----------------------------------------------------------------------------------------------------------------------------------------
--- 2.4 Run the loading procedure for the updated CSV
--------------------------------------------------------------------------------------------------------------------------------------------------
CALL bl_cl.sp_run_batch_etl_by_day('2024-02-15', '2024-02-15');



----------------------------------------------------------------------------------------------------------------------------------------
---2.5 Take screenshot showing updated SCD2 data in 3NF and DM layers

--------------------------------------------------------------------------------------------------------------------------------------------------
select * from bl_3nf.ce_customers_scd ccs where customer_src_id ='EDNA_036' order by ccs.customer_id desc ;

select * from bl_dm.dim_customers_scd dcs where dcs.customer_src_id ='1526' order by dcs.customer_src_id desc ;

SELECT *
FROM bl_3nf.etl_logs
WHERE procedure_name IN (
    'p_load_ce_customers_scd',
    'sp_upsert_dim_customers_scd'
)
order by procedure_name, log_id ;


