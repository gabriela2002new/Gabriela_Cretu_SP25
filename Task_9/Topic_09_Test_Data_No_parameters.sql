-- Step 0: Backup existing data if needed
-- CREATE TABLE bl_dm.fct_sales_backup AS TABLE bl_dm.fct_sales;

----Test that the procedure can be executed repeatedly with consistent results--------------------
--------------------------------------------------------------------------------------------------------------------------
---1.1.Run the procedure:
-----------------------------------------------------------------------------------------------
CALL bl_cl.sp_run_batch_etl_by_day(DATE '2021-01-05', DATE '2021-04-05');---excluding the dm_fct_sales
   
CALL bl_dm.load_fct_sales_rolling_window();---with partitions inside eac loop
call bl_dm.load_fct_sales_rolling_window_no_partition();

select count(*) from bl_dm.fct_sales fs2 ;
select count(*) from bl_3nf.ce_sales cs ;


select count(*) from bl_dm.dim_customers_scd dcs ;
select count(*) from bl_3nf.ce_customers_scd ccs ;

----------------------------------------------------------------------------------------------------------------------------------------
---1.2.Query the logging table for affected rows:
-------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM bl_3nf.etl_logs
WHERE procedure_name IN (
    'sp_insert_fct_sales_incremental'
)
order by procedure_name, log_id ;

select count(*) from bl_3nf.ce_sales cs where event_dt between DATE '2021-01-05'and  DATE '2021-04-05' ;
select count(*) from bl_dm.fct_sales fs2 where event_dt between DATE '2021-01-05'and  DATE '2021-04-05' ;

select * from bl_3nf.ce_sales cs order by event_dt, game_number_id, payment_id, customer_id,retailer_license_number_id, employee_id ;
select * from bl_dm.fct_sales fs2 order by event_dt, game_number_surr_id, payment_method_surr_id, customer_surr_id,retailer_license_number_surr_id, employee_surr_id ;



----------------------------------------------------------------------------------------------------------------------------------------
---1.3.Take a screenshot of the logging result
-----------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------
---1.4. Run the procedure again with the same input
--------------------------------------------------------------------------------------------------------------------------------------------------
CALL bl_dm.load_fct_sales_rolling_window();---with partitions
call bl_dm.load_fct_sales_rolling_window_no_partition();----no partitions

----------------------------------------------------------------------------------------------------------------------------------------
---1.5. Query the login table again
--------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM bl_3nf.etl_logs
WHERE procedure_name IN (
    
    'sp_insert_fct_sales_incremental'
)
order by procedure_name, log_id ;

select count(*) from bl_3nf.ce_sales cs where event_dt between DATE '2021-02-04'and  DATE '2021-02-27' ;
select count(*) from bl_dm.fct_sales fs2 where event_dt between DATE '2021-02-04' and  DATE '2021-02-27' ;

----------------------------------------------------------------------------------------------------------------------------------------
---1.6. Confirm number of rows affected is 0 (or explain why)
----------------------------------------------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------------------------------------------------------
---1.7.Tests for duplication and nulls
----------------------------------------------------------------------------------------------------------------------------------------
---check for duplicates on 3nf
SELECT
    s.game_number_id,
    s.customer_id,
    s.employee_id,
    s.retailer_license_number_id,
    s.payment_id,
    s.event_dt,
    COUNT(*) AS duplicate_count
FROM bl_3nf.ce_sales s
WHERE s.event_dt BETWEEN DATE '2021-01-05' AND DATE '2021-04-05'
GROUP BY
    s.game_number_id,
    s.customer_id,
    s.employee_id,
    s.retailer_license_number_id,
    s.payment_id,
    s.event_dt
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
---check for duplicates on dm
SELECT
    game_number_surr_id,
    customer_surr_id,
    employee_surr_id,
    retailer_license_number_surr_id,
    payment_method_surr_id,
    event_dt,
    COUNT(*) AS row_count
FROM bl_dm.fct_sales
GROUP BY
    game_number_surr_id,
    customer_surr_id,
    employee_surr_id,
    retailer_license_number_surr_id,
    payment_method_surr_id,
    event_dt
HAVING COUNT(*) > 1;

---check for nulls in dct sales on dm
SELECT 
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE game_number_surr_id IS NULL) AS null_game_number,
    COUNT(*) FILTER (WHERE customer_surr_id IS NULL) AS null_customer,
    COUNT(*) FILTER (WHERE employee_surr_id IS NULL) AS null_employee,
    COUNT(*) FILTER (WHERE retailer_license_number_surr_id IS NULL) AS null_retailer,
    COUNT(*) FILTER (WHERE payment_method_surr_id IS NULL) AS null_payment_method
FROM bl_dm.fct_sales;



---Use except to see what rows are in  in DM, but not in 3nf
SELECT 
    dc.customer_src_id,
    dgn.game_number_src_id,
    de.employee_src_id,
    dr.retailer_license_number_src_id,
    dp.payment_method_src_id,
    fs.event_dt
FROM bl_dm.fct_sales fs
LEFT JOIN bl_dm.dim_customers_scd dc ON fs.customer_surr_id = dc.customer_surr_id
LEFT JOIN bl_dm.dim_game_numbers dgn ON fs.game_number_surr_id = dgn.game_number_surr_id
LEFT JOIN bl_dm.dim_employees de  ON fs.employee_surr_id = de.employee_surr_id
LEFT JOIN bl_dm.dim_retailer_license_numbers dr ON fs.retailer_license_number_surr_id = dr.retailer_license_number_surr_id
LEFT JOIN bl_dm.dim_payment_methods dp ON fs.payment_method_surr_id = dp.payment_method_surr_id
EXCEPT
-- Corresponding grain in 3NF
SELECT 
    s.customer_id::TEXT,
    s.game_number_id::TEXT,
    s.employee_id::TEXT,
    s.retailer_license_number_id::TEXT,
    s.payment_id::TEXT,
    s.event_dt
FROM bl_3nf.ce_sales s;



