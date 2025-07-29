 call bl_cl.p_drop_etl_tracker_table();
 call bl_cl.p_create_etl_tracker_table();

----Step 0: Redefine the function from task 8 to only incrementally load everything except for the dm table
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
    --call bl_cl.sp_insert_fct_sales();


    RAISE NOTICE 'All BL_DM data loaded successfully.';
END;
$$;


-- Step 1: Drop existing table (if you can)

DROP TABLE IF EXISTS bl_dm.fct_sales CASCADE;

-- Step 2: Create tables(partitioned and not partitioned)
CREATE table if not exists bl_dm.fct_sales (
    game_number_surr_id BIGINT REFERENCES bl_dm.dim_game_numbers(game_number_surr_id) ON DELETE CASCADE,
    customer_surr_id BIGINT REFERENCES bl_dm.dim_customers_scd(customer_surr_id) ON DELETE CASCADE,
    employee_surr_id BIGINT REFERENCES bl_dm.dim_employees(employee_surr_id) ON DELETE CASCADE,
    retailer_license_number_surr_id BIGINT REFERENCES bl_dm.dim_retailer_license_numbers(retailer_license_number_surr_id) ON DELETE CASCADE,
    payment_method_surr_id BIGINT REFERENCES bl_dm.dim_payment_methods(payment_method_surr_id) ON DELETE CASCADE,
    event_dt DATE NOT NULL,
    ticket_price FLOAT,
    tickets_bought INT,
    payout FLOAT,
    sales FLOAT,
    insert_dt DATE,
    update_dt DATE,

    CONSTRAINT fct_sales_grain_unique UNIQUE (
        game_number_surr_id,
        customer_surr_id,
        employee_surr_id,
        retailer_license_number_surr_id,
        payment_method_surr_id,
        event_dt
    )
) PARTITION BY RANGE (event_dt);



CREATE table if not exists bl_dm.fct_sales (
    game_number_surr_id BIGINT REFERENCES bl_dm.dim_game_numbers(game_number_surr_id) ON DELETE CASCADE,
    customer_surr_id BIGINT REFERENCES bl_dm.dim_customers_scd(customer_surr_id) ON DELETE CASCADE,
    employee_surr_id BIGINT REFERENCES bl_dm.dim_employees(employee_surr_id) ON DELETE CASCADE,
    retailer_license_number_surr_id BIGINT REFERENCES bl_dm.dim_retailer_license_numbers(retailer_license_number_surr_id) ON DELETE CASCADE,
    payment_method_surr_id BIGINT REFERENCES bl_dm.dim_payment_methods(payment_method_surr_id) ON DELETE CASCADE,
    event_dt DATE NOT NULL,
    ticket_price FLOAT,
    tickets_bought INT,
    payout FLOAT,
    sales FLOAT,
    insert_dt DATE,
    update_dt DATE,

    CONSTRAINT fct_sales_grain_unique UNIQUE (
        game_number_surr_id,
        customer_surr_id,
        employee_surr_id,
        retailer_license_number_surr_id,
        payment_method_surr_id,
        event_dt
    )
);

---Step 3: Create rolling window partion for 2 months
CREATE OR REPLACE PROCEDURE bl_dm.manage_partitions_rolling_window(
    p_start_date DATE,
    p_end_date DATE,
    p_partition BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    current_month DATE := date_trunc('month', p_start_date);
    last_month DATE := date_trunc('month', p_end_date);
    partition_name TEXT;
    partition_start DATE;
    partition_end DATE;
BEGIN
    IF NOT p_partition THEN
        RETURN; -- Skip partitioning
    END IF;

    WHILE current_month <= last_month LOOP
        partition_name := 'fct_sales_' || to_char(current_month, 'YYYYMM');
        partition_start := current_month;
        partition_end := current_month + INTERVAL '1 month';

        -- Create partition if not exists
        IF NOT EXISTS (
            SELECT 1 FROM pg_tables 
            WHERE schemaname = 'bl_dm' AND tablename = partition_name
        ) THEN
            EXECUTE format('
                CREATE TABLE bl_dm.%I PARTITION OF bl_dm.fct_sales
                FOR VALUES FROM (%L) TO (%L);',
                partition_name, partition_start::text, partition_end::text);
        END IF;

        current_month := current_month + INTERVAL '1 month';
    END LOOP;
END;
$$;


--Step 4: Bulk insert the 2 month window
CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_fct_sales_incremental(
    p_start_date DATE,
    p_end_date DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted INT;
BEGIN
    WITH ins AS (
        INSERT INTO bl_dm.fct_sales (
            game_number_surr_id,
            customer_surr_id,
            employee_surr_id,
            retailer_license_number_surr_id,
            payment_method_surr_id,
            event_dt,
            ticket_price,
            tickets_bought,
            payout,
            sales,
            insert_dt,
            update_dt
        )
        SELECT
            dgn.game_number_surr_id,
            dc.customer_surr_id,
            de.employee_surr_id,
            dr.retailer_license_number_surr_id,
            dp.payment_method_surr_id,
            dd.event_dt,
            s.ticket_price,
            s.tickets_bought,
            s.payout,
            s.sales,
            s.insert_dt,
            s.update_dt
        FROM bl_3nf.ce_sales s
        LEFT JOIN bl_3nf.ce_game_numbers gn ON gn.game_number_id = s.game_number_id
        LEFT JOIN bl_dm.dim_game_numbers dgn ON dgn.game_number_src_id = gn.game_number_id::TEXT
        JOIN bl_3nf.ce_customers_scd c
          ON c.customer_id = s.customer_id
         AND s.event_dt >= c.start_dt
         AND s.event_dt < c.end_dt
        LEFT JOIN bl_dm.dim_customers_scd dc ON dc.customer_src_id = c.customer_id::TEXT AND dc.start_dt = c.start_dt
        LEFT JOIN bl_3nf.ce_employees e ON e.employee_id = s.employee_id
        LEFT JOIN bl_dm.dim_employees de ON de.employee_src_id = e.employee_id::TEXT
        LEFT JOIN bl_3nf.ce_retailer_license_numbers r ON r.retailer_license_number_id = s.retailer_license_number_id
        LEFT JOIN bl_dm.dim_retailer_license_numbers dr ON dr.retailer_license_number_src_id = r.retailer_license_number_id::TEXT
        LEFT JOIN bl_3nf.ce_payment_methods p ON p.payment_method_id = s.payment_id
        LEFT JOIN bl_dm.dim_payment_methods dp ON dp.payment_method_src_id = p.payment_method_id::TEXT
        LEFT JOIN bl_dm.dim_date dd ON dd.event_dt = s.event_dt
        WHERE s.event_dt BETWEEN p_start_date AND p_end_date
        ON CONFLICT ON CONSTRAINT fct_sales_grain_unique DO NOTHING
        RETURNING 1
    )
    SELECT count(*) INTO v_rows_inserted FROM ins;

    CALL bl_cl.p_log_etl(
        p_procedure_name := 'sp_insert_fct_sales_incremental',
        p_rows_affected := v_rows_inserted,
        p_log_message := format('Inserted %s rows into fct_sales for window %s to %s.', v_rows_inserted, p_start_date, p_end_date),
        p_log_level := 'INFO'
    );
END;
$$;

--Step 5: Create the procedure to recover the last load date
CREATE OR REPLACE PROCEDURE bl_cl.p_get_or_init_tracker_date(
    IN p_table_name TEXT,
    IN p_default_date DATE,
    OUT o_last_processed_date DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Try to get the existing last_processed_date
    SELECT last_processed_date
    INTO o_last_processed_date
    FROM bl_cl.etl_run_tracker
    WHERE table_name = p_table_name;

    -- If not found, insert a default entry and return default date
    IF NOT FOUND THEN
        o_last_processed_date := p_default_date;

        INSERT INTO bl_cl.etl_run_tracker (table_name, last_processed_date)
        VALUES (p_table_name, p_default_date);
    END IF;
END;
$$;

--Step 6: Create the procedure to create a new tracking  table 
CREATE OR REPLACE PROCEDURE bl_cl.p_create_etl_tracker_table()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check if the table already exists
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'bl_cl'
          AND table_name = 'etl_run_tracker'
    ) THEN
        -- Create the tracker table
        EXECUTE $sql$
            CREATE UNLOGGED TABLE bl_cl.etl_run_tracker (
                table_name TEXT PRIMARY KEY,
                last_processed_date DATE NOT NULL,
                load_success boolean, 
                 last_attempted_at timestamp
            )
        $sql$;
    END IF;
END;
$$;

select * from bl_cl.etl_run_tracker;





-- Step 7: define the rolling window procedure for loading data in the fact table without parameteraziation
CREATE OR REPLACE PROCEDURE bl_dm.load_fct_sales_rolling_window()
LANGUAGE plpgsql
AS $$
DECLARE
    v_earliest_date DATE;
    v_latest_date DATE;
    v_last_loaded_date DATE;
    v_window_start DATE;
    v_window_end DATE;
    v_window_size INTERVAL := INTERVAL '2 months';
BEGIN
    -- 1. Get earliest and latest date in source
    SELECT MIN(event_dt), MAX(event_dt)
    INTO v_earliest_date, v_latest_date
    FROM bl_3nf.ce_sales;

    IF v_earliest_date IS NULL OR v_latest_date IS NULL THEN
        RAISE NOTICE 'No source data available.';
        RETURN;
    END IF;

    -- 2. Get last loaded date using tracker procedure
    CALL bl_cl.p_get_or_init_tracker_date(
        'fct_sales',
        (v_earliest_date - INTERVAL '1 day')::date,
        v_last_loaded_date
    );

    -- 3. Set first window start and end dates
    v_window_start := GREATEST(v_last_loaded_date + INTERVAL '1 day', v_earliest_date);
    v_window_end := LEAST(v_window_start + v_window_size - INTERVAL '1 day', v_latest_date);

    -- 4. Loop until we reach the latest date
    WHILE  v_window_start + v_window_size - INTERVAL '1 day' <= v_latest_date
        LOOP

        -- Manage partitions for this window
        CALL bl_dm.manage_partitions_rolling_window(v_window_start, v_window_end, TRUE);

        BEGIN
            -- Insert incremental data for this window
            CALL bl_cl.sp_insert_fct_sales_incremental(v_window_start, v_window_end);

            -- Update tracker with success
            INSERT INTO bl_cl.etl_run_tracker (table_name, last_processed_date, load_success, last_attempted_at)
            VALUES ('fct_sales', v_window_end, TRUE, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name) DO UPDATE
            SET last_processed_date = EXCLUDED.last_processed_date,
                load_success = TRUE,
                last_attempted_at = CURRENT_TIMESTAMP;

            RAISE NOTICE 'Loaded window from % to %', v_window_start, v_window_end;

        EXCEPTION WHEN OTHERS THEN
            -- On failure, update tracker with failure status
            INSERT INTO bl_cl.etl_run_tracker (table_name, last_processed_date, load_success, last_attempted_at)
            VALUES ('fct_sales', v_window_end, FALSE, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name) DO UPDATE
            SET load_success = FALSE,
                last_attempted_at = CURRENT_TIMESTAMP;

            RAISE;
        END;

        -- Move to next window
        v_window_start := v_window_start + INTERVAL '1 day';
        v_window_end := v_window_start + v_window_size - INTERVAL '1 day';
    END LOOP;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_dm.load_fct_sales_rolling_window_no_partition()
LANGUAGE plpgsql
AS $$
DECLARE
    v_earliest_date DATE;
    v_latest_date DATE;
    v_last_loaded_date DATE;
    v_window_start DATE;
    v_window_end DATE;
    v_window_size INTERVAL := INTERVAL '2 months';
BEGIN
    -- 1. Get earliest and latest date in source
    SELECT MIN(event_dt), MAX(event_dt)
    INTO v_earliest_date, v_latest_date
    FROM bl_3nf.ce_sales;

    IF v_earliest_date IS NULL OR v_latest_date IS NULL THEN
        RAISE NOTICE 'No source data available.';
        RETURN;
    END IF;

    -- 2. Get last loaded date using tracker procedure
    CALL bl_cl.p_get_or_init_tracker_date(
        'fct_sales',
        (v_earliest_date - INTERVAL '1 day')::date,
        v_last_loaded_date
    );

    -- 3. Set first window start and end dates
    v_window_start := GREATEST(v_last_loaded_date + INTERVAL '1 day', v_earliest_date);
    v_window_end := LEAST(v_window_start + v_window_size - INTERVAL '1 day', v_latest_date);

    -- 4. Loop until we reach the latest date
    WHILE  v_window_start + v_window_size - INTERVAL '1 day' <= v_latest_date
        LOOP

        -- Manage partitions for this window
        CALL bl_dm.manage_partitions_rolling_window(v_window_start, v_window_end, false);

        BEGIN
            -- Insert incremental data for this window
            CALL bl_cl.sp_insert_fct_sales_incremental(v_window_start, v_window_end);

            -- Update tracker with success
            INSERT INTO bl_cl.etl_run_tracker (table_name, last_processed_date, load_success, last_attempted_at)
            VALUES ('fct_sales', v_window_end, TRUE, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name) DO UPDATE
            SET last_processed_date = EXCLUDED.last_processed_date,
                load_success = TRUE,
                last_attempted_at = CURRENT_TIMESTAMP;

            RAISE NOTICE 'Loaded window from % to %', v_window_start, v_window_end;

        EXCEPTION WHEN OTHERS THEN
            -- On failure, update tracker with failure status
            INSERT INTO bl_cl.etl_run_tracker (table_name, last_processed_date, load_success, last_attempted_at)
            VALUES ('fct_sales', v_window_end, FALSE, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name) DO UPDATE
            SET load_success = FALSE,
                last_attempted_at = CURRENT_TIMESTAMP;

            RAISE;
        END;

        -- Move to next window
        v_window_start := v_window_start + INTERVAL '1 day';
        v_window_end := v_window_start + v_window_size - INTERVAL '1 day';
    END LOOP;
END;
$$;





