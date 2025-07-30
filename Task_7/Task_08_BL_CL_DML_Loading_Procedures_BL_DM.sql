-- ======================================
-- Game Numbers: type 1
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_dim_game_numbers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INT := 0;
BEGIN
    WITH upsert AS (
        INSERT INTO bl_dm.dim_game_numbers (
            game_number_surr_id,
            game_number_src_id,
            game_number_name,
            game_category_id,
            game_category_name,
            game_type_id,
            game_type_name,
            draw_dt,
            average_odds,
            average_odds_prob,
            mid_tier_prize,
            top_tier_prize,
            small_prize,
            winning_chance,
            winning_jackpot,
            source_system,
            source_entity,
            insert_dt,
            update_dt
        )
        SELECT
            nextval('game_number_surr_seq'),
            gn.game_number_id::TEXT,
            gn.game_number_name,
            gc.game_category_id,
            gc.game_category_name,
            gt.game_type_id,
            gt.game_type_name,
            gn.draw_dt,
            gn.average_odds,
            gn.average_odds_prob,
            gn.mid_tier_prize,
            gn.top_tier_prize,
            gn.small_prize,
            gc.winning_chance,
            gc.winning_jackpot,
            'bl_3nf' AS source_system,
            'ce_game_numbers' AS source_entity,
            gn.insert_dt,
            gn.update_dt
        FROM bl_3nf.ce_game_numbers gn
        LEFT JOIN bl_3nf.ce_game_categories gc ON gn.game_category_id = gc.game_category_id
        LEFT JOIN bl_3nf.ce_game_types gt ON gc.game_type_id = gt.game_type_id
        ON CONFLICT (game_number_src_id) DO UPDATE
        SET
            game_number_name   = CASE WHEN EXCLUDED.game_number_name NOT IN ('n. a.', '') THEN EXCLUDED.game_number_name ELSE dim_game_numbers.game_number_name END,
            game_category_id   = CASE WHEN EXCLUDED.game_category_id != -1 THEN EXCLUDED.game_category_id ELSE dim_game_numbers.game_category_id END,
            game_category_name = CASE WHEN EXCLUDED.game_category_name NOT IN ('n. a.', '') THEN EXCLUDED.game_category_name ELSE dim_game_numbers.game_category_name END,
            game_type_id       = CASE WHEN EXCLUDED.game_type_id != -1 THEN EXCLUDED.game_type_id ELSE dim_game_numbers.game_type_id END,
            game_type_name     = CASE WHEN EXCLUDED.game_type_name NOT IN ('n. a.', '') THEN EXCLUDED.game_type_name ELSE dim_game_numbers.game_type_name END,
            draw_dt            = CASE WHEN EXCLUDED.draw_dt != '1900-12-31'::date THEN EXCLUDED.draw_dt ELSE dim_game_numbers.draw_dt END,
            average_odds       = CASE WHEN EXCLUDED.average_odds NOT IN ('n. a.', '') THEN EXCLUDED.average_odds ELSE dim_game_numbers.average_odds END,
            average_odds_prob  = CASE WHEN EXCLUDED.average_odds_prob != -1 AND EXCLUDED.average_odds_prob != -1 THEN EXCLUDED.average_odds_prob ELSE dim_game_numbers.average_odds_prob END,
            mid_tier_prize     = CASE WHEN EXCLUDED.mid_tier_prize != -1 THEN EXCLUDED.mid_tier_prize ELSE dim_game_numbers.mid_tier_prize END,
            top_tier_prize     = CASE WHEN EXCLUDED.top_tier_prize != -1 THEN EXCLUDED.top_tier_prize ELSE dim_game_numbers.top_tier_prize END,
            small_prize        = CASE WHEN EXCLUDED.small_prize != -1 THEN EXCLUDED.small_prize ELSE dim_game_numbers.small_prize END,
            winning_chance     = CASE WHEN EXCLUDED.winning_chance != -1 THEN EXCLUDED.winning_chance ELSE dim_game_numbers.winning_chance END,
            winning_jackpot    = CASE WHEN EXCLUDED.winning_jackpot != -1 THEN EXCLUDED.winning_jackpot ELSE dim_game_numbers.winning_jackpot END,
            insert_dt          = CASE WHEN EXCLUDED.insert_dt != '1900-12-31'::date THEN EXCLUDED.insert_dt ELSE dim_game_numbers.insert_dt END,
            update_dt          =  EXCLUDED.insert_dt
        WHERE
            EXCLUDED.insert_dt > dim_game_numbers.update_dt
            AND (
                dim_game_numbers.game_number_name IS DISTINCT FROM EXCLUDED.game_number_name
                OR dim_game_numbers.game_category_id IS DISTINCT FROM EXCLUDED.game_category_id
                OR dim_game_numbers.game_category_name IS DISTINCT FROM EXCLUDED.game_category_name
                OR dim_game_numbers.game_type_id IS DISTINCT FROM EXCLUDED.game_type_id
                OR dim_game_numbers.game_type_name IS DISTINCT FROM EXCLUDED.game_type_name
                OR dim_game_numbers.draw_dt IS DISTINCT FROM EXCLUDED.draw_dt
                OR dim_game_numbers.average_odds IS DISTINCT FROM EXCLUDED.average_odds
                OR dim_game_numbers.average_odds_prob IS DISTINCT FROM EXCLUDED.average_odds_prob
                OR dim_game_numbers.mid_tier_prize IS DISTINCT FROM EXCLUDED.mid_tier_prize
                OR dim_game_numbers.top_tier_prize IS DISTINCT FROM EXCLUDED.top_tier_prize
                OR dim_game_numbers.small_prize IS DISTINCT FROM EXCLUDED.small_prize
                OR dim_game_numbers.winning_chance IS DISTINCT FROM EXCLUDED.winning_chance
                OR dim_game_numbers.winning_jackpot IS DISTINCT FROM EXCLUDED.winning_jackpot
                OR dim_game_numbers.insert_dt IS DISTINCT FROM EXCLUDED.insert_dt
            )
        RETURNING 1
    )
    SELECT count(*) INTO v_rows_affected FROM upsert;

    call bl_cl.p_log_etl('sp_upsert_dim_game_numbers', v_rows_affected, 'Upsert dim_game_numbers completed successfully.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    call bl_cl.p_log_etl('sp_upsert_dim_game_numbers', 0, 'Error in sp_upsert_dim_game_numbers: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;


-- ======================================
-- Retailer License Numbers: type 1
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_dim_retailer_license_numbers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INT := 0;
BEGIN
    WITH upsert AS (
        INSERT INTO bl_dm.dim_retailer_license_numbers (
            retailer_license_number_surr_id,
            retailer_license_number_src_id,
            retailer_license_number_name,
            retailer_location_name_id,
            retailer_location_name,
            zip_id,
            zip_name,
            city_id,
            city_name,
            state_id,
            state_name,
            source_system,
            source_entity,
            insert_dt,
            update_dt
        )
        SELECT
            NEXTVAL('retailer_license_number_surr_seq'),
            r.retailer_license_number_id::TEXT,
            r.retailer_license_number_name,
            ln.location_name_id,
            ln.location_name,
            z.zip_id,
            z.zip_name,
            c.city_id,
            c.city_name,
            s.state_id,
            s.state_name,
            'bl_3nf' AS source_system,
            'ce_retailer_license_numbers' AS source_entity,
            r.insert_dt,
            r.update_dt
        FROM bl_3nf.ce_retailer_license_numbers r
        LEFT JOIN bl_3nf.ce_location_names ln ON r.retailer_location_name_id = ln.location_name_id
        LEFT JOIN bl_3nf.ce_zip z ON ln.zip_id = z.zip_id
        LEFT JOIN bl_3nf.ce_cities c ON z.city_id = c.city_id
        LEFT JOIN bl_3nf.ce_states s ON c.state_id = s.state_id
        ON CONFLICT (retailer_license_number_src_id) DO UPDATE
SET
    retailer_license_number_name = CASE 
        WHEN EXCLUDED.retailer_license_number_name NOT IN ('n. a.', '') THEN EXCLUDED.retailer_license_number_name 
        ELSE dim_retailer_license_numbers.retailer_license_number_name 
    END,
    retailer_location_name_id    = CASE 
        WHEN EXCLUDED.retailer_location_name_id != -1 THEN EXCLUDED.retailer_location_name_id 
        ELSE dim_retailer_license_numbers.retailer_location_name_id 
    END,
    retailer_location_name       = CASE 
        WHEN EXCLUDED.retailer_location_name NOT IN ('n. a.', '') THEN EXCLUDED.retailer_location_name 
        ELSE dim_retailer_license_numbers.retailer_location_name 
    END,
    zip_id                       = CASE 
        WHEN EXCLUDED.zip_id != -1 THEN EXCLUDED.zip_id 
        ELSE dim_retailer_license_numbers.zip_id 
    END,
    zip_name                     = CASE 
        WHEN EXCLUDED.zip_name NOT IN ('n. a.', '') THEN EXCLUDED.zip_name 
        ELSE dim_retailer_license_numbers.zip_name 
    END,
    city_id                      = CASE 
        WHEN EXCLUDED.city_id != -1 THEN EXCLUDED.city_id 
        ELSE dim_retailer_license_numbers.city_id 
    END,
    city_name                    = CASE 
        WHEN EXCLUDED.city_name NOT IN ('n. a.', '') THEN EXCLUDED.city_name 
        ELSE dim_retailer_license_numbers.city_name 
    END,
    state_id                     = CASE 
        WHEN EXCLUDED.state_id != -1 THEN EXCLUDED.state_id 
        ELSE dim_retailer_license_numbers.state_id 
    END,
    state_name                   = CASE 
        WHEN EXCLUDED.state_name NOT IN ('n. a.', '') THEN EXCLUDED.state_name 
        ELSE dim_retailer_license_numbers.state_name 
    END,
    update_dt                    = EXCLUDED.insert_dt
WHERE 
    EXCLUDED.insert_dt > dim_retailer_license_numbers.update_dt
    AND (
        EXCLUDED.retailer_license_number_name IS DISTINCT FROM dim_retailer_license_numbers.retailer_license_number_name
        OR EXCLUDED.retailer_location_name_id IS DISTINCT FROM dim_retailer_license_numbers.retailer_location_name_id
        OR EXCLUDED.retailer_location_name IS DISTINCT FROM dim_retailer_license_numbers.retailer_location_name
        OR EXCLUDED.zip_id IS DISTINCT FROM dim_retailer_license_numbers.zip_id
        OR EXCLUDED.zip_name IS DISTINCT FROM dim_retailer_license_numbers.zip_name
        OR EXCLUDED.city_id IS DISTINCT FROM dim_retailer_license_numbers.city_id
        OR EXCLUDED.city_name IS DISTINCT FROM dim_retailer_license_numbers.city_name
        OR EXCLUDED.state_id IS DISTINCT FROM dim_retailer_license_numbers.state_id
        OR EXCLUDED.state_name IS DISTINCT FROM dim_retailer_license_numbers.state_name
        OR EXCLUDED.insert_dt IS DISTINCT FROM dim_retailer_license_numbers.insert_dt
        OR EXCLUDED.update_dt IS DISTINCT FROM dim_retailer_license_numbers.update_dt
    )returning 1
)

    SELECT count(*) INTO v_rows_affected FROM upsert;

    call bl_cl.p_log_etl('sp_upsert_dim_retailer_license_numbers', v_rows_affected, 'Upsert dim_retailer_license_numbers completed successfully.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    call bl_cl.p_log_etl('sp_upsert_dim_retailer_license_numbers', 0, 'Error in sp_upsert_dim_retailer_license_numbers: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

-- ======================================
-- Payment Methods:type 0
-- ======================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_dim_payment_methods()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INT := 0;
BEGIN
    WITH upsert AS (
        INSERT INTO bl_dm.dim_payment_methods (
            payment_method_surr_id,
            payment_method_src_id,
            payment_method_name,
            source_system,
            source_entity,
            insert_dt,
            update_dt
        )
        SELECT
            NEXTVAL('payment_method_surr_seq'),
            pm.payment_method_id::TEXT,
            pm.payment_method_name,
            'bl_3nf' AS source_system,
            'ce_payment_methods' AS source_entity,
            pm.insert_dt,
            pm.update_dt
        FROM bl_3nf.ce_payment_methods pm
        ON CONFLICT (payment_method_src_id) DO NOTHING
        RETURNING 1
    )
    SELECT count(*) INTO v_rows_affected FROM upsert;

    call bl_cl.p_log_etl('sp_upsert_dim_payment_methods', v_rows_affected, 'Upsert dim_payment_methods completed successfully.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    call bl_cl.p_log_etl('sp_upsert_dim_payment_methods', 0, 'Error in sp_upsert_dim_payment_methods: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

-- ======================================
-- Employees: type 1
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_dim_employees()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INT := 0;
BEGIN
    WITH upsert AS (
        INSERT INTO bl_dm.dim_employees (
            employee_surr_id,
            employee_src_id,
            employee_name,
            employee_hire_dt,
            employee_status_id,
            employee_status_name,
            employee_department_id,
            employee_department_name,
            employee_email,
            employee_phone,
            employee_salary,
            source_system,
            source_entity,
            insert_dt,
            update_dt
        )
        SELECT
            NEXTVAL('employee_surr_seq'),
            e.employee_id::TEXT,
            e.employee_name,
            e.employee_hire_dt,
            s.status_id,
            s.status_name,
            d.department_id,
            d.department_name,
            e.employee_email,
            e.employee_phone,
            e.employee_salary,
            'bl_3nf' AS source_system,
            'ce_employees' AS source_entity,
            e.insert_dt,
            e.update_dt
        FROM bl_3nf.ce_employees e
        JOIN bl_3nf.ce_statuses s ON e.employee_status_id = s.status_id
        JOIN bl_3nf.ce_departments d ON e.employee_department_id = d.department_id
        ON CONFLICT (employee_src_id) DO UPDATE
        SET
            employee_name = CASE
                WHEN EXCLUDED.employee_name NOT IN ('n. a.', '') THEN EXCLUDED.employee_name
                ELSE dim_employees.employee_name
            END,
            employee_hire_dt = CASE
                WHEN EXCLUDED.employee_hire_dt != '1900-12-31'::date THEN EXCLUDED.employee_hire_dt
                ELSE dim_employees.employee_hire_dt
            END,
            employee_status_id = CASE
                WHEN EXCLUDED.employee_status_id != -1 THEN EXCLUDED.employee_status_id
                ELSE dim_employees.employee_status_id
            END,
            employee_status_name = CASE
                WHEN EXCLUDED.employee_status_name NOT IN ('n. a.', '') THEN EXCLUDED.employee_status_name
                ELSE dim_employees.employee_status_name
            END,
            employee_department_id = CASE
                WHEN EXCLUDED.employee_department_id != -1 THEN EXCLUDED.employee_department_id
                ELSE dim_employees.employee_department_id
            END,
            employee_department_name = CASE
                WHEN EXCLUDED.employee_department_name NOT IN ('n. a.', '') THEN EXCLUDED.employee_department_name
                ELSE dim_employees.employee_department_name
            END,
            employee_email = CASE
                WHEN EXCLUDED.employee_email NOT IN ('n. a.', '') THEN EXCLUDED.employee_email
                ELSE dim_employees.employee_email
            END,
            employee_phone = CASE
                WHEN EXCLUDED.employee_phone NOT IN ('n. a.', '') THEN EXCLUDED.employee_phone
                ELSE dim_employees.employee_phone
            END,
            employee_salary = CASE
                WHEN EXCLUDED.employee_salary != -1 AND EXCLUDED.employee_salary > 0 THEN EXCLUDED.employee_salary
                ELSE dim_employees.employee_salary
            END,
            insert_dt = EXCLUDED.insert_dt,
            update_dt = EXCLUDED.insert_dt 
        WHERE
            EXCLUDED.insert_dt > dim_employees.update_dt
            AND (
                EXCLUDED.employee_name IS DISTINCT FROM dim_employees.employee_name
                OR EXCLUDED.employee_hire_dt IS DISTINCT FROM dim_employees.employee_hire_dt
                OR EXCLUDED.employee_status_id IS DISTINCT FROM dim_employees.employee_status_id
                OR EXCLUDED.employee_status_name IS DISTINCT FROM dim_employees.employee_status_name
                OR EXCLUDED.employee_department_id IS DISTINCT FROM dim_employees.employee_department_id
                OR EXCLUDED.employee_department_name IS DISTINCT FROM dim_employees.employee_department_name
                OR EXCLUDED.employee_email IS DISTINCT FROM dim_employees.employee_email
                OR EXCLUDED.employee_phone IS DISTINCT FROM dim_employees.employee_phone
                OR EXCLUDED.employee_salary IS DISTINCT FROM dim_employees.employee_salary
                OR EXCLUDED.insert_dt IS DISTINCT FROM dim_employees.insert_dt
                OR EXCLUDED.update_dt IS DISTINCT FROM dim_employees.update_dt
            )
        RETURNING 1
    )
    SELECT count(*) INTO v_rows_affected FROM upsert;

    call bl_cl.p_log_etl('sp_upsert_dim_employees', v_rows_affected, 'Upsert dim_employees completed successfully.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    call bl_cl.p_log_etl('sp_upsert_dim_employees', 0, 'Error in sp_upsert_dim_employees: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;


-- ======================================
-- Customers SCD: type 2
-- ======================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_dim_customers_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INT := 0;
    v_customer bl_cl.customer_scd_type;
    ref_cursor REFCURSOR;
    v_last_row_count INT := 0;

BEGIN
    -- Open a cursor for the source data with casts matching the composite type
    OPEN ref_cursor FOR
        SELECT
            c.customer_id::VARCHAR(255) AS customer_src_id,
            c.customer_name,
            c.customer_registration_dt,
            z.zip_id::BIGINT,
            z.zip_name,
            ci.city_id::BIGINT,
            ci.city_name,
            s.state_id::BIGINT,
            s.state_name,
            c.customer_gender,
            c.customer_dob,
            c.customer_email,
            c.customer_phone,
            c.insert_dt::DATE,
            c.start_dt,
            c.end_dt,
            c.is_active
        FROM bl_3nf.ce_customers_scd c
        LEFT JOIN bl_3nf.ce_zip z ON c.customer_zip_code_id = z.zip_id
        LEFT JOIN bl_3nf.ce_cities ci ON z.city_id = ci.city_id
        LEFT JOIN bl_3nf.ce_states s ON ci.state_id = s.state_id;

    -- Loop through the cursor
    LOOP
        FETCH ref_cursor INTO v_customer;
        EXIT WHEN NOT FOUND;

        -- Upsert logic
        INSERT INTO bl_dm.dim_customers_scd (
            customer_surr_id,
            customer_src_id,
            customer_name,
            customer_registration_dt,
            zip_id,
            zip_name,
            city_id,
            city_name,
            state_id,
            state_name,
            customer_gender,
            customer_dob,
            customer_email,
            customer_phone,
            source_system,
            source_entity,
            insert_dt,
            start_dt,
            end_dt,
            is_active
        ) VALUES (
            nextval('customer_surr_seq'),
            v_customer.customer_src_id,
            v_customer.customer_name,
            v_customer.customer_registration_dt,
            v_customer.zip_id,
            v_customer.zip_name,
            v_customer.city_id,
            v_customer.city_name,
            v_customer.state_id,
            v_customer.state_name,
            v_customer.customer_gender,
            v_customer.customer_dob,
            v_customer.customer_email,
            v_customer.customer_phone,
            'bl_3nf',
            'ce_customers_scd',
            v_customer.insert_dt,
            v_customer.start_dt,
            v_customer.end_dt,
            v_customer.is_active
        )
        ON CONFLICT (customer_src_id, start_dt) DO UPDATE
        SET
          is_active = EXCLUDED.is_active,
          end_dt = EXCLUDED.end_dt
        WHERE 
          bl_dm.dim_customers_scd.is_active = true AND
           EXCLUDED.is_active = false;


        -- Track only if an insert or update happened
        GET DIAGNOSTICS v_last_row_count = ROW_COUNT;
        v_rows_affected := v_rows_affected + v_last_row_count;
    END LOOP;

    CLOSE ref_cursor;

    -- Log success
    CALL bl_cl.p_log_etl(
        'sp_upsert_dim_customers_scd',
        v_rows_affected,
        'Upsert dim_customers_scd completed successfully.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.p_log_etl(
        'sp_upsert_dim_customers_scd',
        0,
        'Error in sp_upsert_dim_customers_scd: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;




-- ======================================
-- Sales
-- ======================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_fct_sales()
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
        LEFT JOIN bl_3nf.ce_game_numbers gn 
            ON gn.game_number_id = s.game_number_id
        LEFT JOIN bl_dm.dim_game_numbers dgn 
            ON dgn.game_number_src_id = gn.game_number_id::TEXT
        JOIN (
            SELECT DISTINCT ON (customer_id) *
            FROM bl_3nf.ce_customers_scd
            WHERE is_active = TRUE
            ORDER BY customer_id, start_dt DESC
        ) c 
            ON c.customer_id = s.customer_id 
            AND s.event_dt BETWEEN c.start_dt AND c.end_dt - INTERVAL '1 day'
        LEFT JOIN bl_dm.dim_customers_scd dc 
            ON dc.customer_src_id = c.customer_id::TEXT 
            AND dc.start_dt = c.start_dt
        LEFT JOIN bl_3nf.ce_employees e 
            ON e.employee_id = s.employee_id
        LEFT JOIN bl_dm.dim_employees de 
            ON de.employee_src_id = e.employee_id::TEXT
        LEFT JOIN bl_3nf.ce_retailer_license_numbers r 
            ON r.retailer_license_number_id = s.retailer_license_number_id
        LEFT JOIN bl_dm.dim_retailer_license_numbers dr 
            ON dr.retailer_license_number_src_id = r.retailer_license_number_id::TEXT
        LEFT JOIN bl_3nf.ce_payment_methods p 
            ON p.payment_method_id = s.payment_id
        LEFT JOIN bl_dm.dim_payment_methods dp 
            ON dp.payment_method_src_id = p.payment_method_id::TEXT
        LEFT JOIN bl_dm.dim_date dd 
            ON dd.event_dt = s.event_dt
        ON CONFLICT ON CONSTRAINT fct_sales_grain_unique DO NOTHING
        RETURNING 1
    )
    SELECT count(*) INTO v_rows_inserted FROM ins;

    call bl_cl.p_log_etl(
        p_procedure_name := 'sp_insert_fct_sales',
        p_rows_affected := v_rows_inserted,
        p_log_message := 'Inserted ' || v_rows_inserted || ' rows into fct_sales.',
        p_log_level := 'INFO'
    );

END;
$$;
