CREATE OR REPLACE FUNCTION bl_cl.fn_get_batch_from_source(
    p_schema TEXT,
    p_table TEXT,
    p_start_date DATE,
    p_end_date DATE
)
RETURNS SETOF RECORD
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY EXECUTE FORMAT(
        'SELECT * FROM %I.%I WHERE transaction_dt BETWEEN %L AND %L',
        p_schema, p_table, p_start_date, p_end_date
    );
END;
$$;

CREATE OR REPLACE PROCEDURE  bl_cl.p_load_all_ce_data()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Load all CE data in dependency-respecting order
    CALL bl_cl.p_load_ce_game_types();
    CALL bl_cl.p_load_ce_game_categories();
    CALL bl_cl.p_load_ce_game_numbers();
    
    CALL bl_cl.p_load_ce_payment_methods();
    
    CALL bl_cl.p_load_ce_states();
    CALL bl_cl.p_load_ce_cities();
    CALL bl_cl.p_load_ce_zip();
   CALL bl_cl.p_load_ce_location_names();
    CALL bl_cl.p_load_retailer_license_numbers();

    CALL bl_cl.p_load_ce_statuses();
    CALL bl_cl.p_load_ce_departments();
    CALL bl_cl.p_load_ce_employees();

    CALL bl_cl.p_load_ce_customers_scd();
    CALL bl_cl.p_load_ce_sales();

    RAISE NOTICE 'All CE data loaded successfully.';
END;
$$;
SELECT COUNT(*)
FROM sa_final_draw.src_final_draw sfd
WHERE sfd.transaction_dt::DATE BETWEEN DATE '2022-02-06' AND DATE '2022-02-07';

SELECT COUNT(*)
FROM sa_final_draw.src_final_draw sfd
WHERE sfd.transaction_dt::DATE BETWEEN DATE '2022-02-06' AND DATE '2022-02-07';

SELECT COUNT(*)
FROM sa_final_scratch.src_final_scratch sfd
WHERE sfd.transaction_dt::DATE BETWEEN DATE '2022-02-06' AND DATE '2022-02-07';
SELECT 20994 + 21101;

CREATE OR REPLACE PROCEDURE bl_cl.p_load_all_bl_dm_data()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Load dimensions first in dependency order
    CALL bl_cl.sp_upsert_dim_game_numbers();
    CALL bl_cl.sp_upsert_dim_payment_methods();
    CALL bl_cl.sp_upsert_dim_retailer_license_numbers();
    CALL bl_cl.sp_upsert_dim_employees();
    CALL bl_cl.sp_upsert_dim_customers_scd();

    -- Load fact table after dims are ready
    CALL bl_cl.sp_insert_fct_sales();

    RAISE NOTICE 'All BL_DM data loaded successfully.';
END;
$$;



CREATE OR REPLACE PROCEDURE bl_cl.sp_full_etl_process(
    p_start_date DATE,
    p_end_date DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create temp tables if not exist (using CREATE TEMP TABLE ON COMMIT PRESERVE ROWS)
    -- We'll use ON COMMIT PRESERVE ROWS so they survive across transactions in the session

    
        CREATE TEMP TABLE tmp_scratch_batch (
            transaction_dt VARCHAR,
            retailer_license_number VARCHAR,
            customer_id VARCHAR,
            employee_id VARCHAR,
            game_number VARCHAR,
            tickets_bought VARCHAR,
            payment_method_id VARCHAR,
            sales VARCHAR,
            payout VARCHAR,
            customer_name VARCHAR,
            customer_gender VARCHAR,
            customer_dob VARCHAR,
            employee_name VARCHAR,
            employee_department VARCHAR,
            employee_status VARCHAR,
            retailer_location_name VARCHAR,
            retailer_location_zip_code VARCHAR,
            retailer_location_city VARCHAR,
            ticket_price VARCHAR,
            game_category VARCHAR,
            game_type VARCHAR,
            average_odds VARCHAR,
            average_odds_prob VARCHAR,
            top_prize VARCHAR,
            mid_prize VARCHAR,
            small_prize VARCHAR,
            payment_method_name VARCHAR
        ) ;

    -- Load data into tmp_scratch_batch
    INSERT INTO tmp_scratch_batch
    SELECT * FROM bl_cl.fn_get_batch_from_source(
        'sa_final_scratch',
        'src_final_scratch',
        p_start_date,
        p_end_date
    ) AS t (
        transaction_dt VARCHAR,
        retailer_license_number VARCHAR,
        customer_id VARCHAR,
        employee_id VARCHAR,
        game_number VARCHAR,
        tickets_bought VARCHAR,
        payment_method_id VARCHAR,
        sales VARCHAR,
        payout VARCHAR,
        customer_name VARCHAR,
        customer_gender VARCHAR,
        customer_dob VARCHAR,
        employee_name VARCHAR,
        employee_department VARCHAR,
        employee_status VARCHAR,
        retailer_location_name VARCHAR,
        retailer_location_zip_code VARCHAR,
        retailer_location_city VARCHAR,
        ticket_price VARCHAR,
        game_category VARCHAR,
        game_type VARCHAR,
        average_odds VARCHAR,
        average_odds_prob VARCHAR,
        top_prize VARCHAR,
        mid_prize VARCHAR,
        small_prize VARCHAR,
        payment_method_name VARCHAR
    );

    -- Temp table for draw batch
 
        CREATE TEMP TABLE tmp_draw_batch (
            transaction_dt VARCHAR,
            retailer_license_number VARCHAR,
            customer_id VARCHAR,
            employee_id VARCHAR,
            game_number VARCHAR,
            payment_method_id VARCHAR,
            sales VARCHAR,
            tickets_bought VARCHAR,
            payout VARCHAR,
            customer_email VARCHAR,
            customer_phone VARCHAR,
            customer_registration_dt VARCHAR,
            customer_state VARCHAR,
            customer_city VARCHAR,
            customer_zip_code VARCHAR,
            employee_email VARCHAR,
            employee_phone VARCHAR,
            employee_hire_dt VARCHAR,
            employee_salary VARCHAR,
            retailer_location_name VARCHAR,
            retailer_location_zip_code VARCHAR,
            retailer_location_state VARCHAR,
            game_category VARCHAR,
            game_type VARCHAR,
            ticket_price VARCHAR, 
            winning_chance VARCHAR,
            winning_jackpot VARCHAR,
            draw_dt VARCHAR,
            payment_method_name VARCHAR
        ) ;

    -- Load data into tmp_draw_batch
    INSERT INTO tmp_draw_batch
    SELECT * FROM bl_cl.fn_get_batch_from_source(
        'sa_final_draw',
        'src_final_draw',
        p_start_date,
        p_end_date
    ) AS t (
        transaction_dt VARCHAR,
        retailer_license_number VARCHAR,
        customer_id VARCHAR,
        employee_id VARCHAR,
        game_number VARCHAR,
        payment_method_id VARCHAR,
        sales VARCHAR,
        tickets_bought VARCHAR,
        payout VARCHAR,
        customer_email VARCHAR,
        customer_phone VARCHAR,
        customer_registration_dt VARCHAR,
        customer_state VARCHAR,
        customer_city VARCHAR,
        customer_zip_code VARCHAR,
        employee_email VARCHAR,
        employee_phone VARCHAR,
        employee_hire_dt VARCHAR,
        employee_salary VARCHAR,
        retailer_location_name VARCHAR,
        retailer_location_zip_code VARCHAR,
        retailer_location_state VARCHAR,
        game_category VARCHAR,
        game_type VARCHAR,
        ticket_price VARCHAR, 
        winning_chance VARCHAR,
        winning_jackpot VARCHAR,
        draw_dt VARCHAR,
        payment_method_name VARCHAR
    );

    -- Now call all ETL procedures in sequence

    CALL bl_cl.sp_insert_meta_game_types(); 
    CALL bl_cl.sp_load_wrk_game_types();
    CALL bl_cl.sp_upsert_lkp_game_types();

    CALL bl_cl.sp_insert_meta_game_categories();
    CALL bl_cl.sp_load_wrk_game_categories();
    CALL bl_cl.sp_upsert_lkp_game_categories();

     CALL bl_cl.sp_insert_meta_game_numbers();
    CALL bl_cl.sp_load_wrk_game_numbers();
    CALL bl_cl.sp_upsert_lkp_game_numbers();

    CALL bl_cl.sp_insert_meta_payment_methods();
    CALL bl_cl.sp_load_wrk_payment_methods();
    CALL bl_cl.sp_upsert_lkp_payment_methods();
    
    CALL bl_cl.sp_insert_meta_states();
    CALL bl_cl.sp_load_wrk_states();
    CALL bl_cl.sp_upsert_lkp_states();

    CALL bl_cl.sp_insert_meta_cities();
    CALL bl_cl.sp_load_wrk_cities();
    CALL bl_cl.sp_upsert_lkp_cities();

    CALL bl_cl.sp_insert_meta_zips();
    CALL bl_cl.sp_load_wrk_zips();
    CALL bl_cl.sp_upsert_lkp_zips();

    CALL bl_cl.sp_insert_meta_location_names();
    CALL bl_cl.sp_load_wrk_location_names();
    CALL bl_cl.sp_upsert_lkp_location_names();

    CALL bl_cl.sp_insert_meta_retailers();
    CALL bl_cl.sp_load_wrk_retailers();
    CALL bl_cl.sp_upsert_lkp_retailers();

    CALL bl_cl.sp_insert_meta_statuses();
    CALL bl_cl.sp_load_wrk_statuses();
    CALL bl_cl.sp_upsert_lkp_statuses();

    CALL bl_cl.sp_insert_meta_departments();
    CALL bl_cl.sp_load_wrk_departments();
    CALL bl_cl.sp_upsert_lkp_departments();

    CALL bl_cl.sp_insert_meta_employees();
    CALL bl_cl.sp_load_wrk_employees();
    CALL bl_cl.sp_upsert_lkp_employees();

    CALL bl_cl.sp_insert_meta_customers();
    CALL bl_cl.sp_load_wrk_customers();
    CALL bl_cl.sp_upsert_lkp_customers();

    CALL bl_cl.sp_insert_meta_sales();
    CALL bl_cl.sp_load_wrk_sales();
    CALL bl_cl.sp_upsert_lkp_sales();
   
    DROP TABLE IF EXISTS tmp_scratch_batch;
    DROP TABLE IF EXISTS tmp_draw_batch;



END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.sp_run_batch_etl_by_day(
    p_start_date DATE,
    p_end_date DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_date DATE;
BEGIN
    v_current_date := p_start_date;

    WHILE v_current_date <= p_end_date LOOP
        RAISE NOTICE 'Processing ETL for date: %', v_current_date;

        -- Run full ETL process for the specific day
        CALL bl_cl.sp_full_etl_process(v_current_date, v_current_date);

        -- After full ETL, load CE reference data into 3NF layer
        CALL  bl_cl.p_load_all_ce_data();

        -- After full ETL, load BL_DM dimension and fact data
        CALL bl_cl.p_load_all_bl_dm_data();


        -- Move to next day
        v_current_date := v_current_date + INTERVAL '1 day';
    END LOOP;

    RAISE NOTICE 'ETL and 3NF loading completed for all dates from % to %', p_start_date, p_end_date;
END;
$$;



select *
from   sa_final_scratch.src_final_scratch sfs
where sfs.transaction_dt ='2024-02-15';

select *
from   sa_final_scratch.src_final_scratch sfs
where sfs.customer_id ='EDNA_036';

select *
from   sa_final_draw.src_final_draw sfd
where sfd.customer_id ='EDNA_036';

select *
from bl_3nf.ce_customers_scd  ccs 
where ccs.customer_src_id ='EDNA_036';


select *
from bl_cl.lkp_customers lc 
where customer_src_id ='EDNA_036';

select *
from bl_3nf.ce_employees ce  where employee_src_id='EMP_100035_3';

select *
from bl_cl.lkp_employees le where employee_src_id ='EMP_100035_3';
where customer_src_id ='EDNA_036';



CALL bl_cl.sp_full_etl_process('2021-01-07', '2021-01-07');
call bl_3nf.p_load_all_ce_data();

CALL bl_cl.sp_full_etl_process('2021-01-08', '2021-01-08');
call bl_3nf.p_load_all_ce_data();
select * from bl_cl.lkp_customers lc where customer_src_id ='EDNA_036' order by lc.customer_id desc;
select * from bl_3nf.ce_customers_scd ccs where customer_src_id ='EDNA_036' order by ccs.customer_id desc ;
select * from bl_dm.dim_customers_scd dcs where dcs.customer_src_id ='1526' order by dcs.customer_src_id desc ;

select * from bl_cl.lkp_cities lc2 order by lc2.city_id desc ;
select * from bl_3nf.ce_cities cc ;order by cc.city_id desc;

select * from bl_cl.lkp_departments ld order by ld.department_id desc;
select * from bl_3nf.ce_departments cd;order by cd.department_id desc;

select * from bl_cl.lkp_employees le where employee_src_id='EMP_100035_3';
select * from bl_3nf.ce_employees ce where employee_src_id='EMP_100035_3';-----
_
select * from bl_cl.lkp_game_categories lgc order by lgc.game_category_id  desc;
select * from bl_3nf.ce_game_categories cgc order by cgc.game_category_id desc;

select * from bl_cl.lkp_game_numbers lgn order by lgn.game_number_id desc;
select * from bl_3nf.ce_game_numbers cgn; order by cgn.game_number_id desc ;
select * from bl_dm.dim_game_numbers cgn where cgn.game_number_surr_id in (940,937) ;


select * from bl_cl.lkp_game_types lgt order by lgt.game_type_id desc;
select * from bl_3nf.ce_game_types cgt order by cgt.game_type_id desc;


select * from bl_cl.lkp_location_names lln order by lln.location_name_id desc ;
select * from bl_3nf.ce_location_names cln; order by cln.location_name_id desc ;

select * from bl_cl.lkp_payment_methods lpm order by lpm.payment_method_id desc;
select * from bl_3nf.ce_payment_methods cpm order by cpm.payment_method_id desc;

select * from bl_cl.lkp_retailers lr order by lr.retailer_license_number_id desc ;
select * from bl_3nf.ce_retailer_license_numbers crln; order by crln.retailer_license_number_id desc;

select * from bl_cl.lkp_sales ls  order by ls.game_number_src_id desc ;
select * from bl_3nf.ce_sales cs where customer_id=1526 order by cs.game_number_id desc;
select * from bl_dm.fct_sales fs where customer_surr_id in (854,14465) order by fs.game_number_surr_id desc;

select * from bl_3nf.ce_sales cs where customer_id is null;
select count(*) from bl_3nf.ce_sales; cs where customer_id is null;
select count(*) from bl_dm.fct_sales fs2; where customer_surr_id is not null;
select * from bl_dm.fct_sales fs where customer_surr_id is null; in (854,14465) order by fs.game_number_surr_id desc;

select * from bl_dm.fct_sales fs where payment_method_surr_id is null;

select * from bl_cl.lkp_states ls order by ls.state_id desc ;
select * from bl_3nf.ce_states cs order by cs.state_id desc;

select * from bl_cl.lkp_statuses ls order by ls.status_id desc;
select * from bl_3nf.ce_statuses cs order by cs.status_id desc ;

select * from bl_cl.lkp_zips lz order by lz.zip_id desc ;
select * from bl_3nf.ce_zip cz where cz.zip_name ='n. a.'; order by cz.zip_id desc ;



select count(*) from bl_dm.dim_customers_scd dcs;
select count(*) from bl_3nf.ce_customers_scd dcs;






CALL bl_cl.sp_run_batch_etl_by_day(DATE '2022-02-06', DATE '2022-02-07');
call bl_cl.sp_run_batch_etl_by_day(DATE '2024-02-15', DATE '2024-02-15') ;





