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
    'p_load_ce_sales'
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
    'p_load_ce_sales'
)
order by procedure_name, log_id ;

----------------------------------------------------------------------------------------------------------------------------------------
---1.6. Confirm number of rows affected is 0 (or explain why)
----------------------------------------------------------------------------------------------------------------------------------------



