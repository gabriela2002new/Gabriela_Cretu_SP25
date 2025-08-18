CREATE OR REPLACE PROCEDURE bl_cl.drop_bl_cl_objects()
LANGUAGE plpgsql
AS $$
DECLARE
    v_table TEXT;
    v_tables TEXT[] := ARRAY[
        'lkp_cities',
        'lkp_customers',
        'lkp_departments',
        'lkp_employees',
        'lkp_game_categories',
        'lkp_game_numbers',
        'lkp_game_types',
        'lkp_location_names',
        'lkp_payment_methods',
        'lkp_retailers',
        'lkp_sales',
        'lkp_states',
        'lkp_statuses',
        'lkp_zips',
        'mta_cities',
        'mta_customers',
        'mta_departments',
        'mta_employees',
        'mta_game_categories',
        'mta_game_numbers',
        'mta_game_types',
        'mta_location_names',
        'mta_payment_methods',
        'mta_retailers',
        'mta_sales',
        'mta_states',
        'mta_statuses',
        'mta_zips',
        'wrk_cities',
        'wrk_customers',
        'wrk_departments',
        'wrk_employees',
        'wrk_game_categories',
        'wrk_game_numbers',
        'wrk_game_types',
        'wrk_location_names',
        'wrk_payment_methods',
        'wrk_retailers',
        'wrk_sales',
        'wrk_states',
        'wrk_statuses',
        'wrk_zips'
    ];
BEGIN
    FOREACH v_table IN ARRAY v_tables LOOP
        EXECUTE FORMAT('DROP TABLE IF EXISTS bl_cl.%I CASCADE;', v_table);
        RAISE NOTICE 'Dropped table: bl_cl.%', v_table;
    END LOOP;
END;
$$;



CREATE OR REPLACE PROCEDURE bl_cl.drop_bl_3nf_sequences()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP SEQUENCE IF EXISTS bl_cl.t_map_game_types_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.ce_game_categories_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.ce_game_numbers_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.ce_payment_methods_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.ce_states_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.ce_cities_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.ce_zips_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.ce_location_names_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.t_mapping_retailer_license_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.t_mapping_status_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.t_mapping_department_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.t_mapping_employees_seq CASCADE;
    DROP SEQUENCE IF EXISTS bl_cl.t_mapping_customers_seq CASCADE;

    RAISE NOTICE 'All sequences dropped successfully.';
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.drop_bl_3nf_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS
        bl_3nf.ce_sales,
        bl_3nf.ce_customers_scd,
        bl_3nf.ce_employees,
        bl_3nf.ce_departments,
        bl_3nf.ce_statuses,
        bl_3nf.ce_retailer_license_numbers,
        bl_3nf.ce_location_names,
        bl_3nf.ce_zip,
        bl_3nf.ce_cities,
        bl_3nf.ce_states,
        bl_3nf.ce_payment_methods,
        bl_3nf.ce_game_numbers,
        bl_3nf.ce_game_categories,
        bl_3nf.ce_game_types
    CASCADE;

    RAISE NOTICE 'All ce_3NF tables dropped successfully.';
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.drop_p_log_etl()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS
     
        bl_3nf.etl_logs

    CASCADE;

    RAISE NOTICE 'The logging table have been dropped';
END;
$$;
CREATE OR REPLACE PROCEDURE bl_cl.drop_bl_dm_sequences()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Drop all surrogate key sequences if they exist
    PERFORM 1 FROM pg_class WHERE relkind = 'S' AND relname = 'game_number_surr_seq';
    IF FOUND THEN EXECUTE 'DROP SEQUENCE IF EXISTS game_number_surr_seq'; END IF;

    PERFORM 1 FROM pg_class WHERE relkind = 'S' AND relname = 'customer_surr_seq';
    IF FOUND THEN EXECUTE 'DROP SEQUENCE IF EXISTS customer_surr_seq'; END IF;

    PERFORM 1 FROM pg_class WHERE relkind = 'S' AND relname = 'retailer_license_number_surr_seq';
    IF FOUND THEN EXECUTE 'DROP SEQUENCE IF EXISTS retailer_license_number_surr_seq'; END IF;

    PERFORM 1 FROM pg_class WHERE relkind = 'S' AND relname = 'employee_surr_seq';
    IF FOUND THEN EXECUTE 'DROP SEQUENCE IF EXISTS employee_surr_seq'; END IF;

    PERFORM 1 FROM pg_class WHERE relkind = 'S' AND relname = 'payment_method_surr_seq';
    IF FOUND THEN EXECUTE 'DROP SEQUENCE IF EXISTS payment_method_surr_seq'; END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.drop_bl_dm_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Drop fact table first due to foreign key constraints
    DROP TABLE IF EXISTS bl_dm.fct_sales CASCADE;
    DROP TABLE IF EXISTS bl_dm.dim_payment_methods CASCADE;
    DROP TABLE IF EXISTS bl_dm.dim_employees CASCADE;
    DROP TABLE IF EXISTS bl_dm.dim_customers_scd CASCADE;
    DROP TABLE IF EXISTS bl_dm.dim_retailer_license_numbers CASCADE;
    DROP TABLE IF EXISTS bl_dm.dim_game_numbers CASCADE;


    -- Drop schema if empty
    

    RAISE NOTICE 'DM objects dropped successfully.';
END;
$$;




CREATE OR REPLACE PROCEDURE bl_cl.drop_customer_scd_type_if_exists()
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema TEXT := 'bl_cl';
    v_type_name TEXT := 'customer_scd_type';
    v_full_type TEXT := format('%I.%I', v_schema, v_type_name);
BEGIN
    -- Check if the type exists
    IF EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = v_type_name
          AND n.nspname = v_schema
    ) THEN
        EXECUTE format('DROP TYPE %s', v_full_type);
    END IF;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.drop_customer_src_id_unique_constraint()
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ce_customers_scd_customer_src_id_key'
    ) THEN
        EXECUTE 'ALTER TABLE bl_3nf.ce_customers_scd DROP CONSTRAINT ce_customers_scd_customer_src_id_key';
        RAISE NOTICE 'Unique constraint ce_customers_scd_customer_src_id_key dropped.';
    ELSE
        RAISE NOTICE 'Unique constraint ce_customers_scd_customer_src_id_key does not exist.';
    END IF;
END;
$$;




CREATE OR REPLACE PROCEDURE bl_cl.reset_all_objects()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL bl_cl.drop_bl_cl_objects();
    CALL bl_cl.drop_bl_3nf_sequences();
    CALL bl_cl.drop_bl_3nf_objects();
    CALL bl_cl.drop_bl_dm_sequences();
    CALL bl_cl.drop_bl_dm_objects();
    CALL bl_cl.drop_p_log_etl();
    call bl_cl.drop_customer_scd_type_if_exists();
END;
$$;

CALL bl_cl.reset_all_objects();



