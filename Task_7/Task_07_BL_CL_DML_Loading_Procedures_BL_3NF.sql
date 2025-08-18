-- ======================================
-- Game Types: type 0
-- ======================================

CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_game_types()
RETURNS TABLE (
    game_type_id BIGINT,
    game_type_src_id VARCHAR,
    game_type_name VARCHAR,
    source_system VARCHAR,
    source_entity VARCHAR,
    insert_dt DATE,
    update_dt DATE
)
LANGUAGE plpgsql
AS $$
DECLARE rec RECORD;
BEGIN
    FOR rec IN
        SELECT DISTINCT lgt.game_type_src_id
        FROM bl_cl.lkp_game_types lgt
        WHERE lgt.game_type_src_id IS NOT NULL
    LOOP
        RETURN QUERY
        SELECT 
            MIN(lgt.game_type_id)::BIGINT,
            rec.game_type_src_id,
            COALESCE(
                (SELECT lg.game_type_name
                 FROM bl_cl.lkp_game_types lg
                 WHERE lg.game_type_src_id = rec.game_type_src_id
                   AND TRIM(lg.game_type_name) IS NOT NULL
                   AND TRIM(lg.game_type_name) <> ''
                   AND LOWER(TRIM(lg.game_type_name)) <> 'n. a.'
                 ORDER BY lg.insert_dt ASC
                 LIMIT 1),
                'n. a.'
            ),
            'bl_cl'::VARCHAR,
            'lkp_game_types'::VARCHAR,
            MIN(lgt.insert_dt),
            MAX(lgt.update_dt)
        FROM bl_cl.lkp_game_types lgt
        WHERE lgt.game_type_src_id = rec.game_type_src_id;
    END LOOP;
END;
$$;

SELECT *
    FROM bl_cl.f_get_merged_game_types();

CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_game_types()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INT;
BEGIN
    INSERT INTO bl_3nf.CE_GAME_TYPES (
        GAME_TYPE_ID,
        GAME_TYPE_SRC_ID,
        GAME_TYPE_NAME,
        SOURCE_SYSTEM,
        SOURCE_ENTITY,
        INSERT_DT,
        UPDATE_DT
    )
    SELECT *
    from bl_cl.f_get_merged_game_types()
    ON CONFLICT (game_type_src_id) DO nothing;
   

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_cl.p_log_etl(
        'p_load_ce_game_types',
        v_rows,
        'Successfully loaded game types.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.p_log_etl(
            'p_load_ce_game_types',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CALL bl_cl.p_load_ce_game_types();
SELECT * FROM bl_3nf.CE_GAME_TYPES;
select * from bl_3nf.etl_logs;


-- ======================================
-- Game Categories:type 1
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_game_categories()
RETURNS TABLE (
    game_category_id BIGINT,
    game_category_src_id VARCHAR,
    game_type_id BIGINT,
    game_category_name VARCHAR,
    winning_chance NUMERIC,
    winning_jackpot NUMERIC,
    source_system VARCHAR,
    source_entity VARCHAR,
    insert_dt DATE,
    update_dt DATE
)LANGUAGE sql
AS $$
    SELECT
        MAX(gc.game_category_id) AS game_category_id,gc.game_category_src_id,gt.game_type_id,COALESCE(MAX(NULLIF(gc.game_category_name, 'n. a.')), 'n. a.') AS game_category_name,
        COALESCE(MAX(NULLIF(gc.winning_chance, -1)), -1) AS winning_chance,
        COALESCE(MAX(NULLIF(gc.winning_jackpot, -1)), -1) AS winning_jackpot,
        'bl_cl' AS source_system,
        'lkp_game_categories' AS source_entity,
        MIN(gc.insert_dt) AS insert_dt,
        MAX(gc.update_dt) AS update_dt
    FROM bl_cl.lkp_game_categories gc
    LEFT JOIN bl_3nf.CE_GAME_TYPES gt
      ON gt.game_type_name = gc.game_type_name
    GROUP BY gc.game_category_src_id, gt.game_type_id
$$;


CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_game_categories()
LANGUAGE plpgsql
AS $$
DECLARE 
    v_rows INT;
BEGIN
    INSERT INTO bl_3nf.CE_GAME_CATEGORIES (
        game_category_id,
        game_category_src_id,
        game_type_id,
        game_category_name,
        winning_chance,
        winning_jackpot,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT *
    FROM bl_cl.f_get_merged_game_categories()
    ON CONFLICT (game_category_src_id) DO UPDATE
SET
    game_type_id = EXCLUDED.game_type_id,
    game_category_name = EXCLUDED.game_category_name,
    winning_chance = EXCLUDED.winning_chance,
    winning_jackpot = EXCLUDED.winning_jackpot,
    update_dt = EXCLUDED.update_dt
WHERE
    EXCLUDED.insert_dt > CE_GAME_CATEGORIES.update_dt
    AND (
        CE_GAME_CATEGORIES.game_type_id IS DISTINCT FROM EXCLUDED.game_type_id
        OR CE_GAME_CATEGORIES.game_category_name IS DISTINCT FROM EXCLUDED.game_category_name
        OR CE_GAME_CATEGORIES.winning_chance IS DISTINCT FROM EXCLUDED.winning_chance
        OR CE_GAME_CATEGORIES.winning_jackpot IS DISTINCT FROM EXCLUDED.winning_jackpot
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_cl.p_log_etl(
        'p_load_ce_game_categories',
        v_rows,
        'Successfully loaded game categories.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.p_log_etl(
            'p_load_ce_game_categories',
            0,
            'Error loading game categories: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;


-- Call the procedure to load data
CALL bl_cl.p_load_ce_game_categories();

-- Check results
SELECT * FROM bl_3nf.CE_GAME_CATEGORIES;
select * from bl_3nf.etl_logs;




-- ======================================
-- Game Numbers: type 1
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_game_numbers()
RETURNS TABLE (
    game_number_id BIGINT,
    game_number_src_id VARCHAR,
    game_category_id BIGINT,
    draw_dt DATE,
    game_number_name VARCHAR,
    average_odds VARCHAR,
    average_odds_prob NUMERIC,
    mid_tier_prize NUMERIC,
    top_tier_prize NUMERIC,
    small_prize NUMERIC,
    source_system VARCHAR,
    source_entity VARCHAR,
    insert_dt DATE,
    update_dt DATE
)
LANGUAGE sql
AS $$
    SELECT
        MAX(gn.game_number_id) AS game_number_id,
        gn.game_number_src_id,
        cat.game_category_id,
        COALESCE(MAX(NULLIF(gn.draw_dt, DATE '1900-01-01')), DATE '1900-01-01') AS draw_dt,
        COALESCE(MAX(NULLIF(gn.game_number_name, 'n. a.')), 'n. a.') AS game_number_name,
        COALESCE(MAX(NULLIF(gn.average_odds, 'n. a.')), 'n. a.') AS average_odds,
        COALESCE(MAX(NULLIF(gn.average_odds_prob, -1)), -1) AS average_odds_prob,
        COALESCE(MAX(NULLIF(gn.mid_tier_prize, -1)), -1) AS mid_tier_prize,
        COALESCE(MAX(NULLIF(gn.top_tier_prize, -1)), -1) AS top_tier_prize,
        COALESCE(MAX(NULLIF(gn.small_prize, -1)), -1) AS small_prize,
        'bl_cl' AS source_system,
        'lkp_game_numbers' AS source_entity,
        MIN(gn.insert_dt) AS insert_dt,
        MAX(gn.update_dt) AS update_dt
    FROM bl_cl.lkp_game_numbers gn
    LEFT JOIN bl_3nf.CE_GAME_CATEGORIES cat
      ON cat.game_category_name = gn.game_category_name
    GROUP BY gn.game_number_src_id, cat.game_category_id
$$;


CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_game_numbers()
LANGUAGE plpgsql
AS $$
DECLARE 
    v_rows INT;
BEGIN
    INSERT INTO bl_3nf.CE_GAME_NUMBERS (
        game_number_id,
        game_number_src_id,
        game_category_id,
        draw_dt,
        game_number_name,
        average_odds,
        average_odds_prob,
        mid_tier_prize,
        top_tier_prize,
        small_prize,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT *
    FROM bl_cl.f_get_merged_game_numbers()
    ON CONFLICT (game_number_src_id) DO UPDATE
SET
    game_category_id = EXCLUDED.game_category_id,
    draw_dt = EXCLUDED.draw_dt,
    game_number_name = EXCLUDED.game_number_name,
    average_odds = EXCLUDED.average_odds,
    average_odds_prob = EXCLUDED.average_odds_prob,
    mid_tier_prize = EXCLUDED.mid_tier_prize,
    top_tier_prize = EXCLUDED.top_tier_prize,
    small_prize = EXCLUDED.small_prize,
    update_dt = EXCLUDED.update_dt
WHERE
    EXCLUDED.insert_dt > CE_GAME_NUMBERS.update_dt
    AND (
        CE_GAME_NUMBERS.game_category_id IS DISTINCT FROM EXCLUDED.game_category_id
        OR CE_GAME_NUMBERS.draw_dt IS DISTINCT FROM EXCLUDED.draw_dt
        OR CE_GAME_NUMBERS.game_number_name IS DISTINCT FROM EXCLUDED.game_number_name
        OR CE_GAME_NUMBERS.average_odds IS DISTINCT FROM EXCLUDED.average_odds
        OR CE_GAME_NUMBERS.average_odds_prob IS DISTINCT FROM EXCLUDED.average_odds_prob
        OR CE_GAME_NUMBERS.mid_tier_prize IS DISTINCT FROM EXCLUDED.mid_tier_prize
        OR CE_GAME_NUMBERS.top_tier_prize IS DISTINCT FROM EXCLUDED.top_tier_prize
        OR CE_GAME_NUMBERS.small_prize IS DISTINCT FROM EXCLUDED.small_prize
        OR CE_GAME_NUMBERS.source_system IS DISTINCT FROM EXCLUDED.source_system
        OR CE_GAME_NUMBERS.source_entity IS DISTINCT FROM EXCLUDED.source_entity
    );


    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_cl.p_log_etl(
        'p_load_ce_game_numbers',
        v_rows,
        'Successfully loaded game numbers.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.p_log_etl(
            'p_load_ce_game_numbers',
            0,
            'Error loading game numbers: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

-- Call the procedure to load the data
CALL bl_cl.p_load_ce_game_numbers();

-- Verify results
SELECT * FROM bl_3nf.CE_GAME_NUMBERS sen ORDER BY game_number_id;
select count(*)
from bl_3nf.CE_GAME_NUMBERS;
;where sen.game_number_src_id='Scratch_100099-65432-9876'; 


-- ======================================
-- Payment Methods:type 0
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_payment_methods()
RETURNS TABLE (
    payment_method_id BIGINT,
    payment_method_src_id VARCHAR,
    payment_method_name VARCHAR,
    source_system VARCHAR,
    source_entity VARCHAR,
    insert_dt DATE,
    update_dt DATE
)
LANGUAGE sql
AS $$
    SELECT
        MAX(payment_method_id) AS payment_method_id,
        payment_method_src_id,
        COALESCE(MAX(NULLIF(TRIM(payment_method_name), 'n. a.')), 'n. a.') AS payment_method_name,
        'bl_cl' AS source_system,
        'lkp_payment_methods' AS source_entity,
        MIN(insert_dt) AS insert_dt,
        MAX(update_dt) AS update_dt
    FROM bl_cl.lkp_payment_methods
    GROUP BY payment_method_src_id
$$;

CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_payment_methods()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INT;
BEGIN
    INSERT INTO bl_3nf.CE_PAYMENT_METHODS (
        payment_method_id,
        payment_method_src_id,
        payment_method_name,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT *
    FROM bl_cl.f_get_merged_payment_methods()
    ON CONFLICT (payment_method_src_id) DO NOTHING;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_cl.p_log_etl(
        'p_load_ce_payment_methods',
        v_rows,
        'Successfully loaded payment methods.',
        'INFO'
    );
EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.p_log_etl(
            'p_load_ce_payment_methods',
            0,
            'Error loading payment methods: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

-- Call the procedure to load the data
CALL bl_cl.p_load_ce_payment_methods();

-- Verify results
SELECT * FROM bl_3nf.ce_payment_methods ORDER BY payment_method_id;
SELECT COUNT(*) FROM bl_3nf.ce_payment_methods;
select * FROM bl_cl.lkp_payment_methods lpm ;
-- ======================================
-- States:type 0
-- =======================================

CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_states()
RETURNS TABLE (
    state_id BIGINT,
    state_src_id VARCHAR,
    state_name VARCHAR,
    source_system VARCHAR,
    source_entity VARCHAR,
    insert_dt DATE,
    update_dt DATE
)
LANGUAGE sql
AS $$
    SELECT
        MAX(state_id) AS state_id,
        state_src_id,
        COALESCE(MAX(NULLIF(TRIM(state_name), 'n. a.')), 'n. a.') AS state_name,
        'bl_cl' AS source_system,
        'lkp_states' AS source_entity,
        MIN(insert_dt) AS insert_dt,
        MAX(update_dt) AS update_dt
    FROM bl_cl.lkp_states
    GROUP BY state_src_id
$$;

CREATE OR REPLACE procedure bl_cl.p_load_ce_states()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INT;
BEGIN
    INSERT INTO bl_3nf.CE_STATES (
        state_id,
        state_src_id,
        state_name,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT *
    FROM bl_cl.f_get_merged_states()
    ON CONFLICT (state_src_id) DO NOTHING;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    call bl_cl.p_log_etl(
        'p_load_ce_states',
        v_rows,
        'Successfully loaded states.',
        'INFO'
    );
EXCEPTION
    WHEN OTHERS THEN
        call bl_cl.p_log_etl(
            'p_load_ce_states',
            0,
            'Error loading states: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;


CALL bl_cl.p_load_ce_states();
SELECT * FROM bl_3nf.ce_states ORDER BY state_id;
SELECT * FROM bl_cl.lkp_states ORDER BY state_src_id;



-- ======================================
-- Cities:type 1
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_cities()
RETURNS TABLE (
    city_id BIGINT,
    city_src_id VARCHAR,
    city_name VARCHAR,
    state_name VARCHAR,
    source_system VARCHAR,
    source_entity VARCHAR,
    insert_dt DATE,
    update_dt DATE
)
LANGUAGE sql
AS $$
    SELECT
        MAX(city_id) AS city_id,
        city_src_id,
        COALESCE(MAX(NULLIF(TRIM(city_name), 'n. a.')), 'n. a.') AS city_name,
        COALESCE(MAX(NULLIF(TRIM(state_name), 'n. a.')),'n. a.') AS state_name,
        'bl_cl' AS source_system,
        'lkp_cities' AS source_entity,
        MIN(insert_dt) AS insert_dt,
        MAX(update_dt) AS update_dt
    FROM bl_cl.lkp_cities
    GROUP BY city_src_id
$$;


CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_cities()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_state_id BIGINT;
    v_row_count INT := 0;  -- Initialize to 0
    v_inserted INT;
BEGIN
    FOR rec IN
        SELECT * FROM bl_cl.f_get_merged_cities()
    LOOP
        BEGIN
            -- Resolve state_id from CE_STATES
            SELECT state_id
            INTO v_state_id
            FROM bl_3nf.CE_STATES
            WHERE TRIM(state_name) = TRIM(COALESCE(rec.state_name, ''))
            LIMIT 1;

            IF v_state_id IS NULL THEN
                v_state_id := -1;
            END IF;

            -- Insert into CE_CITIES
            INSERT INTO bl_3nf.CE_CITIES (
                city_id,
                city_src_id,
                state_id,
                city_name,
                source_system,
                source_entity,
                insert_dt,
                update_dt
            )
            VALUES (
                rec.city_id,
                rec.city_src_id,
                v_state_id,
                COALESCE(rec.city_name, 'n. a.'),
                rec.source_system,
                rec.source_entity,
                rec.insert_dt,
                rec.update_dt
            )
            ON CONFLICT (city_src_id) DO UPDATE
SET
    state_id = CASE WHEN EXCLUDED.state_id != -1 THEN EXCLUDED.state_id ELSE CE_CITIES.state_id END,
    city_name = CASE WHEN EXCLUDED.city_name NOT IN ('n. a.', '') THEN EXCLUDED.city_name ELSE CE_CITIES.city_name END,
    update_dt = EXCLUDED.insert_dt
WHERE
    EXCLUDED.insert_dt > CE_CITIES.update_dt
    AND
    (CE_CITIES.state_id IS DISTINCT FROM EXCLUDED.state_id OR CE_CITIES.city_name IS DISTINCT FROM EXCLUDED.city_name);
           GET DIAGNOSTICS v_inserted = ROW_COUNT;
            v_row_count := v_row_count + v_inserted;

        EXCEPTION
            WHEN OTHERS THEN
                CALL bl_cl.p_log_etl(
                    'p_load_ce_cities',
                    0,
                    'Error inserting city_src_id = ' || rec.city_src_id || ': ' || SQLERRM,
                    'ERROR'
                );
        END;
    END LOOP;

    -- Log total inserted row count
    CALL bl_cl.p_log_etl(
        'p_load_ce_cities',
        v_row_count,
        'Successfully loaded cities.',
        'INFO'
    );
END;
$$;


-- Execute the load
CALL bl_cl.p_load_ce_cities();

-- Verify the data
SELECT * FROM bl_3nf.ce_cities ORDER BY city_id;



-- ======================================
-- Zip Codes:type 1
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_zips()
RETURNS TABLE (
    zip_id BIGINT,
    zip_src_id VARCHAR,
    zip_name VARCHAR,
    city_name VARCHAR,
    source_system VARCHAR,
    source_entity VARCHAR,
    insert_dt DATE,
    update_dt DATE
)
LANGUAGE sql
AS $$
    SELECT
        MAX(zip_id) AS zip_id,
        zip_src_id,
        COALESCE(MAX(NULLIF(TRIM(zip_name), 'n. a.')), 'n. a.') AS zip_name,
        COALESCE(MAX(NULLIF(TRIM(city_name), 'n. a.')), 'n. a.') AS city_name,
        'bl_cl' AS source_system,
        'lkp_zips' AS source_entity,
        MIN(insert_dt) AS insert_dt,
        MAX(update_dt) AS update_dt
    FROM bl_cl.lkp_zips
    GROUP BY zip_src_id
$$;

CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_zip()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_city_id BIGINT;
    v_row_count INT := 0;     -- Track total inserted rows
    v_inserted INT;
BEGIN
    FOR rec IN
        SELECT * FROM bl_cl.f_get_merged_zips()
    LOOP
        BEGIN
            -- Resolve city_id from CE_CITIES
            SELECT city_id
            INTO v_city_id
            FROM bl_3nf.CE_CITIES
            WHERE TRIM(city_name) = TRIM(COALESCE(rec.city_name, ''))
            LIMIT 1;

            -- Fallback if not found
            IF v_city_id IS NULL THEN
                v_city_id := -1;
            END IF;

            -- Insert into CE_ZIP
            INSERT INTO bl_3nf.CE_ZIP (
                zip_id,
                zip_src_id,
                city_id,
                zip_name,
                source_system,
                source_entity,
                insert_dt,
                update_dt
            )
            VALUES (
                rec.zip_id,
                rec.zip_src_id,
                v_city_id,
                COALESCE(rec.zip_name, 'n. a.'),
                rec.source_system,
                rec.source_entity,
                rec.insert_dt,
                rec.update_dt
            )
            ON CONFLICT (zip_src_id) DO UPDATE
            SET
                city_id = CASE WHEN EXCLUDED.city_id != -1 THEN EXCLUDED.city_id ELSE CE_ZIP.city_id END,
                zip_name = CASE WHEN EXCLUDED.zip_name NOT IN ('n. a.', '') THEN EXCLUDED.zip_name ELSE CE_ZIP.zip_name END,
                update_dt =  EXCLUDED.insert_dt
            WHERE
                EXCLUDED.insert_dt > CE_ZIP.update_dt
                AND (
                    CE_ZIP.city_id IS DISTINCT FROM EXCLUDED.city_id
                    OR CE_ZIP.zip_name IS DISTINCT FROM EXCLUDED.zip_name
                );


            -- Track inserted row
            GET DIAGNOSTICS v_inserted = ROW_COUNT;
            v_row_count := v_row_count + v_inserted;

        EXCEPTION
            WHEN OTHERS THEN
                CALL bl_cl.p_log_etl(
                    'p_load_ce_zip',
                    0,
                    'Error inserting zip_src_id = ' || rec.zip_src_id || ': ' || SQLERRM,
                    'ERROR'
                );
        END;
    END LOOP;

    -- Final ETL log
    CALL bl_cl.p_log_etl(
        'p_load_ce_zip',
        v_row_count,
        'Successfully loaded ZIPs.',
        'INFO'
    );
END;
$$;



CALL bl_cl.p_load_ce_zip();

SELECT * FROM bl_3nf.ce_zip cz; where cz.zip_src_id= '79936';


-- ======================================
-- Location Names: type 1
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_location_names()
RETURNS TABLE (
    location_name_id BIGINT,
    location_name_src_id VARCHAR,
    location_name VARCHAR,
    zip_name VARCHAR,
    insert_dt DATE,
    update_dt DATE,
    source_system VARCHAR,
    source_entity VARCHAR
)
LANGUAGE sql
AS $$
    SELECT
        location_name_id,
        location_name_src_id,
        TRIM(location_name) AS location_name,
        TRIM(zip_name) AS zip_name,
        insert_dt,
        update_dt,
        'bl_cl' AS source_system,
        'lkp_location_names' AS source_entity
    FROM bl_cl.lkp_location_names
    GROUP BY
        location_name_id,
        location_name_src_id,
        location_name,
        zip_name,
        insert_dt,
        update_dt
$$;

CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_location_names()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_zip_id BIGINT;
    v_row_count INT := 0;  -- total inserted rows
    v_inserted INT;        -- inserted for current row
BEGIN
    FOR rec IN
        SELECT * FROM bl_cl.f_get_merged_location_names()
    LOOP
        BEGIN
            -- Resolve zip_id from CE_ZIP
            SELECT zip_id
            INTO v_zip_id
            FROM bl_3nf.CE_ZIP
            WHERE TRIM(zip_name) = TRIM(COALESCE(rec.zip_name, ''))
            LIMIT 1;

            -- Fallback if no match found
            IF v_zip_id IS NULL THEN
                v_zip_id := -1;
            END IF;

            -- Insert into CE_LOCATION_NAMES
            INSERT INTO bl_3nf.CE_LOCATION_NAMES (
                location_name_id,
                location_name_src_id,
                zip_id,
                location_name,
                source_system,
                source_entity,
                insert_dt,
                update_dt
            )
            VALUES (
                rec.location_name_id,
                rec.location_name_src_id,
                v_zip_id,
                COALESCE(rec.location_name, 'n. a.'),
                rec.source_system,
                rec.source_entity,
                rec.insert_dt,
                rec.update_dt
            )
            ON CONFLICT (location_name_src_id) DO UPDATE
            SET
                zip_id = CASE WHEN EXCLUDED.zip_id != -1 THEN EXCLUDED.zip_id ELSE CE_LOCATION_NAMES.zip_id END,
                location_name = CASE WHEN EXCLUDED.location_name NOT IN ('n. a.', '') THEN EXCLUDED.location_name ELSE CE_LOCATION_NAMES.location_name END,
                update_dt = EXCLUDED.insert_dt
            WHERE
                EXCLUDED.insert_dt > CE_LOCATION_NAMES.update_dt
                AND (CE_LOCATION_NAMES.zip_id IS DISTINCT FROM EXCLUDED.zip_id OR CE_LOCATION_NAMES.location_name IS DISTINCT FROM EXCLUDED.location_name);-- Track inserted row
            GET DIAGNOSTICS v_inserted = ROW_COUNT;
            v_row_count := v_row_count + v_inserted;

        EXCEPTION
            WHEN OTHERS THEN
                CALL bl_cl.p_log_etl(
                    'p_load_ce_location_names',
                    0,
                    'Error inserting location_name_src_id = ' || rec.location_name_src_id || ': ' || SQLERRM,
                    'ERROR'
                );
        END;
    END LOOP;

    -- Final log
    CALL bl_cl.p_log_etl(
        'p_load_ce_location_names',
        v_row_count,
        'Successfully loaded location names.',
        'INFO'
    );
END;
$$;


CALL bl_cl.p_load_ce_location_names();

SELECT * FROM bl_3nf.CE_LOCATION_NAMES; ORDER BY location_name_id;




-- ======================================
-- Retailer License Numbers: type 1
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_retailer_license_numbers()
RETURNS TABLE (
    retailer_license_number_id BIGINT,
    retailer_license_number_src_id VARCHAR,
    location_name VARCHAR,
    retailer_license_number_name VARCHAR,
    source_system VARCHAR,
    source_entity VARCHAR,
    insert_dt DATE,
    update_dt DATE
)
LANGUAGE sql
AS $$
    SELECT
        retailer_license_number_id,
        retailer_license_number_src_id,
        COALESCE(
            MAX(CASE WHEN TRIM(location_name) NOT IN ('n. a.') THEN TRIM(location_name) END),
            'n. a.'
        ) AS location_name,
        COALESCE(
            MAX(CASE WHEN TRIM(retailer_license_number_name) NOT IN ('n. a.') THEN TRIM(retailer_license_number_name) END),
            'n. a.'
        ) AS retailer_license_number_name,
        'bl_cl' AS source_system,
        'lkp_retailers' AS source_entity,
        MIN(insert_dt) AS insert_dt,
        MAX(update_dt) AS update_dt
    FROM bl_cl.lkp_retailers
    GROUP BY retailer_license_number_src_id, retailer_license_number_id
$$;



CREATE OR REPLACE PROCEDURE bl_cl.p_load_retailer_license_numbers()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_location_id BIGINT;
    v_row_count INT := 0;     -- total inserted
    v_inserted INT;           -- inserted per row
BEGIN
    FOR rec IN
        SELECT * FROM bl_cl.f_get_merged_retailer_license_numbers()
    LOOP
        BEGIN
            -- Resolve location_name_id from CE_LOCATION_NAMES
            SELECT location_name_id
            INTO v_location_id
            FROM bl_3nf.CE_LOCATION_NAMES
            WHERE TRIM(location_name) = TRIM(COALESCE(rec.location_name, ''))
            LIMIT 1;

            -- Fallback if not found
            IF v_location_id IS NULL THEN
                v_location_id := -1;
            END IF;

            -- Insert into CE_RETAILER_LICENSE_NUMBERS
            INSERT INTO bl_3nf.CE_RETAILER_LICENSE_NUMBERS (
                retailer_license_number_id,
                retailer_license_number_src_id,
                retailer_location_name_id,
                retailer_license_number_name,
                source_system,
                source_entity,
                insert_dt,
                update_dt
            )
            VALUES (
                rec.retailer_license_number_id,
                rec.retailer_license_number_src_id,
                v_location_id,
                COALESCE(rec.retailer_license_number_name, 'n. a.'),
                rec.source_system,
                rec.source_entity,
                rec.insert_dt,
                rec.update_dt
            )
            ON CONFLICT (retailer_license_number_src_id) DO UPDATE
            SET
                retailer_location_name_id = CASE WHEN EXCLUDED.retailer_location_name_id != -1 THEN EXCLUDED.retailer_location_name_id ELSE CE_RETAILER_LICENSE_NUMBERS.retailer_location_name_id END,
                retailer_license_number_name = CASE WHEN EXCLUDED.retailer_license_number_name NOT IN ('n. a.', '') THEN EXCLUDED.retailer_license_number_name ELSE CE_RETAILER_LICENSE_NUMBERS.retailer_license_number_name END,
                update_dt = EXCLUDED.insert_dt
            WHERE
                EXCLUDED.insert_dt > CE_RETAILER_LICENSE_NUMBERS.update_dt
                AND (
                    CE_RETAILER_LICENSE_NUMBERS.retailer_location_name_id IS DISTINCT FROM EXCLUDED.retailer_location_name_id
                    OR CE_RETAILER_LICENSE_NUMBERS.retailer_license_number_name IS DISTINCT FROM EXCLUDED.retailer_license_number_name
                );
            -- Count successful inserts
            GET DIAGNOSTICS v_inserted = ROW_COUNT;
            v_row_count := v_row_count + v_inserted;

        EXCEPTION
            WHEN OTHERS THEN
                CALL bl_cl.p_log_etl(
                    'p_load_retailer_license_numbers',
                    0,
                    'Error inserting retailer_license_number_src_id = ' || rec.retailer_license_number_src_id || ': ' || SQLERRM,
                    'ERROR'
                );
        END;
    END LOOP;

    -- Log final success count
    CALL bl_cl.p_log_etl(
        'p_load_retailer_license_numbers',
        v_row_count,
        'Successfully loaded retailer license numbers.',
        'INFO'
    );
END;
$$;


CALL bl_cl.p_load_retailer_license_numbers();

SELECT * FROM bl_3nf.CE_RETAILER_LICENSE_NUMBERS ORDER BY retailer_license_number_id;



-- ======================================
-- Statuses: type 0
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_statuses()
RETURNS TABLE (
    status_id BIGINT,
    status_src_id VARCHAR,
    status_name VARCHAR,
    insert_dt DATE,
    update_dt DATE,
    source_system VARCHAR,
    source_entity VARCHAR
)
LANGUAGE sql
AS $$
    SELECT
        status_id,
        status_src_id,
        COALESCE(
            MAX(CASE WHEN TRIM(status_name) NOT IN ('n. a.') THEN TRIM(status_name) END),
            'n. a.'
        ) AS status_name,
        MIN(insert_dt) AS insert_dt,
        MAX(update_dt) AS update_dt,
        'bl_cl' AS source_system,
        'lkp_statuses' AS source_entity
    FROM bl_cl.lkp_statuses
    GROUP BY status_src_id, status_id
$$;

CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_statuses()
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count INT;
BEGIN
    BEGIN
        INSERT INTO bl_3nf.CE_STATUSES (
            status_id,
            status_src_id,
            status_name,
            source_system,
            source_entity,
            insert_dt,
            update_dt
        )
        SELECT
            status_id,
            status_src_id,
            status_name,
            source_system,
            source_entity,
            insert_dt,
            update_dt
        FROM bl_cl.f_get_merged_statuses()
        ON CONFLICT (status_src_id) DO NOTHING;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;

        CALL bl_cl.p_log_etl(
            'p_load_ce_statuses',
            v_row_count,
            'Successfully loaded statuses.',
            'INFO'
        );
    EXCEPTION
        WHEN OTHERS THEN
            CALL bl_cl.p_log_etl(
                'p_load_ce_statuses',
                0,
                'Error loading CE_STATUSES: ' || SQLERRM,
                'ERROR'
            );
            RAISE;
    END;
END;
$$;

CALL bl_cl.p_load_ce_statuses();

SELECT * FROM bl_3nf.CE_STATUSES ORDER BY status_id;


-- ======================================
---Departments:type 0
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_departments()
RETURNS TABLE (
    department_id BIGINT,
    department_src_id VARCHAR,
    department_name VARCHAR,
    insert_dt DATE,
    update_dt DATE,
    source_system VARCHAR,
    source_entity VARCHAR
)
LANGUAGE sql
AS $$
    SELECT
        department_id,
        department_src_id,
        COALESCE(
            MAX(CASE WHEN TRIM(department_name) NOT IN ('n. a.') THEN TRIM(department_name) END),
            'n. a.'
        ) AS department_name,
        MIN(insert_dt) AS insert_dt,
        MAX(update_dt) AS update_dt,
        'bl_cl' AS source_system,
        'lkp_departments' AS source_entity
    FROM bl_cl.lkp_departments
    GROUP BY department_src_id, department_id
$$;


CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_departments()
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count INT;
BEGIN
    BEGIN
        INSERT INTO bl_3nf.CE_DEPARTMENTS (
            department_id,
            department_src_id,
            department_name,
            source_system,
            source_entity,
            insert_dt,
            update_dt
        )
        SELECT
            department_id,
            department_src_id,
            department_name,
            source_system,
            source_entity,
            insert_dt,
            update_dt
        FROM bl_cl.f_get_merged_departments()
        ON CONFLICT (department_src_id) DO NOTHING;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;

        CALL bl_cl.p_log_etl(
            'p_load_ce_departments',
            v_row_count,
            'Successfully loaded departments.',
            'INFO'
        );
    EXCEPTION
        WHEN OTHERS THEN
            CALL bl_cl.p_log_etl(
                'p_load_ce_departments',
                0,
                'Error loading CE_DEPARTMENTS: ' || SQLERRM,
                'ERROR'
            );
            RAISE;
    END;
END;
$$;


CALL bl_cl.p_load_ce_departments();

SELECT * FROM bl_3nf.CE_DEPARTMENTS ORDER BY department_id;


-- ======================================
-- Employees: type 1
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_employees()
RETURNS TABLE (
    employee_id BIGINT,
    employee_src_id VARCHAR,
    department_name VARCHAR,
    status_name VARCHAR,
    employee_name VARCHAR,
    employee_email VARCHAR,
    employee_phone VARCHAR,
    employee_salary NUMERIC,
    employee_hire_dt DATE,
    insert_dt DATE,
    update_dt DATE,
    source_system VARCHAR,
    source_entity VARCHAR
)
LANGUAGE sql
AS $$
    SELECT
        employee_id,
        employee_src_id,
        COALESCE(MAX(CASE WHEN TRIM(employee_department_name) NOT IN ('n. a.') THEN TRIM(employee_department_name) END),'n. a.') AS department_name,
        COALESCE(MAX(CASE WHEN TRIM(employee_status_name) NOT IN ('n. a.') THEN TRIM(employee_status_name) END),'n. a.') AS status_name,
        COALESCE(MAX(CASE WHEN TRIM(employee_name) NOT IN ('n. a.') THEN TRIM(employee_name) END),'n. a.') AS employee_name,
        COALESCE(MAX(CASE WHEN TRIM(employee_email) NOT IN ('n. a.') THEN TRIM(employee_email) END),'n. a.') AS employee_email,
        COALESCE(MAX(CASE WHEN TRIM(employee_phone) NOT IN ('n. a.') THEN TRIM(employee_phone) END),'n. a.') AS employee_phone,
        MAX(employee_salary) AS employee_salary,
        MAX(employee_hire_dt) AS employee_hire_dt,
        MIN(insert_dt) AS insert_dt,
        MAX(update_dt) AS update_dt,
        'bl_cl' AS source_system,
        'lkp_employees' AS source_entity
    FROM bl_cl.lkp_employees
    GROUP BY employee_id, employee_src_id
$$;


CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_employees()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_department_id BIGINT;
    v_status_id BIGINT;
    v_row_count INT := 0;
    v_inserted INT;
BEGIN
    FOR rec IN SELECT * FROM bl_cl.f_get_merged_employees()
    LOOP
        BEGIN
            -- Resolve department_id from CE_DEPARTMENTS
            SELECT department_id INTO v_department_id FROM bl_3nf.CE_DEPARTMENTS WHERE TRIM(department_name) = TRIM(COALESCE(rec.department_name, '')) LIMIT 1;
            IF v_department_id IS NULL THEN v_department_id := -1;
            END IF;
          -- Resolve status_id from CE_STATUSES
            SELECT status_id INTO v_status_id FROM bl_3nf.CE_STATUSES WHERE TRIM(status_name) = TRIM(COALESCE(rec.status_name, '')) LIMIT 1;
            IF v_status_id IS NULL THEN v_status_id := -1;
            END IF;
            -- Insert into CE_EMPLOYEES table
            INSERT INTO bl_3nf.CE_EMPLOYEES (
                employee_id,
                employee_src_id,
                employee_department_id,
                employee_status_id,
                employee_hire_dt,
                employee_name,
                employee_email,
                employee_phone,
                employee_salary,
                source_system,
                source_entity,
                insert_dt,
                update_dt
            )
            VALUES (
                rec.employee_id,
                rec.employee_src_id,
                v_department_id,
                v_status_id,
                rec.employee_hire_dt,
                rec.employee_name,
                rec.employee_email,
                rec.employee_phone,
                rec.employee_salary,
                rec.source_system,
                rec.source_entity,
                rec.insert_dt,
                rec.update_dt
            )
            ON CONFLICT (employee_src_id) DO UPDATE
            SET
                employee_department_id = CASE WHEN EXCLUDED.employee_department_id != -1 THEN EXCLUDED.employee_department_id ELSE CE_EMPLOYEES.employee_department_id END,
                employee_status_id = CASE WHEN EXCLUDED.employee_status_id != -1 THEN EXCLUDED.employee_status_id ELSE CE_EMPLOYEES.employee_status_id END,
                employee_hire_dt = CASE WHEN EXCLUDED.employee_hire_dt != '1900-12-31'::date THEN EXCLUDED.employee_hire_dt ELSE CE_EMPLOYEES.employee_hire_dt END,
                employee_name = CASE WHEN EXCLUDED.employee_name NOT IN ('n. a.', '') THEN EXCLUDED.employee_name ELSE CE_EMPLOYEES.employee_name END,
                employee_email = CASE WHEN EXCLUDED.employee_email NOT IN ('n. a.', '') THEN EXCLUDED.employee_email ELSE CE_EMPLOYEES.employee_email END,
                employee_phone = CASE WHEN EXCLUDED.employee_phone NOT IN ('n. a.', '') THEN EXCLUDED.employee_phone ELSE CE_EMPLOYEES.employee_phone END,
                employee_salary = CASE WHEN EXCLUDED.employee_salary IS NOT NULL AND EXCLUDED.employee_salary > 0 THEN EXCLUDED.employee_salary ELSE CE_EMPLOYEES.employee_salary END,
                update_dt = EXCLUDED.insert_dt
            WHERE
                EXCLUDED.insert_dt > CE_EMPLOYEES.update_dt
                AND (
                    CE_EMPLOYEES.employee_department_id IS DISTINCT FROM EXCLUDED.employee_department_id
                    OR CE_EMPLOYEES.employee_status_id IS DISTINCT FROM EXCLUDED.employee_status_id
                    OR CE_EMPLOYEES.employee_hire_dt IS DISTINCT FROM EXCLUDED.employee_hire_dt
                    OR CE_EMPLOYEES.employee_name IS DISTINCT FROM EXCLUDED.employee_name
                    OR CE_EMPLOYEES.employee_email IS DISTINCT FROM EXCLUDED.employee_email
                    OR CE_EMPLOYEES.employee_phone IS DISTINCT FROM EXCLUDED.employee_phone
                    OR CE_EMPLOYEES.employee_salary IS DISTINCT FROM EXCLUDED.employee_salary
                );
            GET DIAGNOSTICS v_inserted = ROW_COUNT;
            v_row_count := v_row_count + v_inserted;

        EXCEPTION WHEN OTHERS THEN
            CALL bl_cl.p_log_etl(
                'p_load_ce_employees',
                0,
                'Error processing employee_src_id = ' || rec.employee_src_id || ': ' || SQLERRM,
                'ERROR'
            );
        END;
    END LOOP;

    CALL bl_cl.p_log_etl(
        'p_load_ce_employees',
        v_row_count,
        'Successfully loaded employees.',
        'INFO'
    );
END;
$$;

-- ======================================
-- Customers SCD: type 2
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_customers_scd()
RETURNS TABLE (
    customer_src_id VARCHAR,
    customer_id BIGINT,
    zip_name VARCHAR,
    customer_registration_dt DATE,
    customer_name VARCHAR,
    customer_gender VARCHAR,
    customer_dob DATE,
    customer_email VARCHAR,
    customer_phone VARCHAR,
    start_dt DATE,
    insert_dt DATE,
    end_dt DATE,
    is_active BOOLEAN,
    source_system VARCHAR,
    source_entity VARCHAR
)
LANGUAGE sql
AS $$
WITH active_customers AS (
    SELECT *
    FROM bl_cl.lkp_customers
    WHERE is_active = TRUE
),
cleaned AS (
    SELECT
        customer_id,
        MIN(customer_src_id) AS customer_src_id,
        
        -- Clean up values by excluding placeholders
        MAX(NULLIF(zip_name, 'n. a.')) AS zip_name,
        MAX(NULLIF(customer_name, 'n. a.')) AS customer_name,
        MAX(NULLIF(customer_gender, 'n. a.')) AS customer_gender,
        MAX(NULLIF(customer_email, 'n. a.')) AS customer_email,
        MAX(NULLIF(customer_phone, 'n. a.')) AS customer_phone,

        -- Handle default placeholder dates
        MAX(NULLIF(customer_registration_dt, DATE '1900-01-01')) AS customer_registration_dt,
        MAX(NULLIF(customer_dob, DATE '1900-01-01')) AS customer_dob,

        -- Choose latest start_dt and insert_dt
        MAX(start_dt) AS start_dt,
        MAX(insert_dt) AS insert_dt
    FROM active_customers
    GROUP BY customer_id
)
SELECT
    customer_src_id,
    customer_id,
    COALESCE(zip_name, 'n. a.') AS zip_name,
    coalesce(customer_registration_dt, DATE '1900-01-01'),
    COALESCE(customer_name, 'n. a.') AS customer_name,
    COALESCE(customer_gender, 'n. a.') AS customer_gender,
    coalesce(customer_dob, DATE '1900-01-01'),
    COALESCE(customer_email, 'n. a.') AS customer_email,
    COALESCE(customer_phone, 'n. a.') AS customer_phone,
    start_dt,
    insert_dt,
    DATE '9999-12-31' AS end_dt,
    TRUE AS is_active,
    'bl_cl' AS source_system,
    'lkp_customers' AS source_entity
FROM cleaned
$$;



CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_customers_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_zip_id BIGINT;
    v_surr_id BIGINT;
    v_row_count INT := 0;
BEGIN
    FOR rec IN SELECT * FROM bl_cl.f_get_merged_customers_scd()
    LOOP
        BEGIN
            -- Resolve ZIP ID
            SELECT zip_id INTO v_zip_id FROM bl_3nf.CE_ZIP WHERE zip_name = rec.zip_name LIMIT 1; -- Check if active customer already exists
            IF EXISTS (
                SELECT 1 FROM bl_3nf.CE_CUSTOMERS_SCD WHERE customer_src_id = rec.customer_src_id AND source_system = rec.source_system AND source_entity = rec.source_entity 
                AND is_active = TRUE
            ) THEN -- Check if all attributes are the same (no changes)
                IF EXISTS (
                    SELECT 1
                    FROM bl_3nf.CE_CUSTOMERS_SCD
                    WHERE customer_src_id = rec.customer_src_id
                      AND source_system = rec.source_system
                      AND source_entity = rec.source_entity
                      AND is_active = TRUE
                      AND customer_name IS NOT DISTINCT FROM rec.customer_name
                      AND customer_email IS NOT DISTINCT FROM rec.customer_email
                      AND customer_phone IS NOT DISTINCT FROM rec.customer_phone
                      AND customer_gender IS NOT DISTINCT FROM rec.customer_gender
                      AND customer_dob IS NOT DISTINCT FROM rec.customer_dob
                      AND customer_registration_dt IS NOT DISTINCT FROM rec.customer_registration_dt
                      AND customer_zip_code_id IS NOT DISTINCT FROM v_zip_id
                ) THEN
                    -- No change: skip to next record
                    CONTINUE;
                ELSE
                    -- Get surrogate key from existing row
                    SELECT customer_id INTO v_surr_id
                    FROM bl_3nf.CE_CUSTOMERS_SCD
                    WHERE customer_src_id = rec.customer_src_id
                      AND source_system = rec.source_system
                      AND source_entity = rec.source_entity
                      AND is_active = TRUE
                    LIMIT 1;

                    -- Expire existing active record
                    UPDATE bl_3nf.CE_CUSTOMERS_SCD
                    SET is_active = FALSE,
                        end_dt = rec.start_dt
                    WHERE customer_src_id = rec.customer_src_id
                      AND source_system = rec.source_system
                      AND source_entity = rec.source_entity
                      AND is_active = TRUE;

                    -- Insert new version
                    INSERT INTO bl_3nf.CE_CUSTOMERS_SCD (
                        customer_id,
                        customer_src_id,
                        customer_name,
                        customer_email,
                        customer_phone,
                        customer_gender,
                        customer_zip_code_id,
                        customer_dob,
                        customer_registration_dt,
                        insert_dt,
                        start_dt,
                        end_dt,
                        is_active,
                        source_system,
                        source_entity
                    ) VALUES (
                        v_surr_id,
                        rec.customer_src_id,
                        rec.customer_name,
                        rec.customer_email,
                        rec.customer_phone,
                        rec.customer_gender,
                        v_zip_id,
                        rec.customer_dob,
                        rec.customer_registration_dt,
                        rec.start_dt,
                        rec.start_dt,
                        DATE '9999-12-31',
                        TRUE,
                        rec.source_system,
                        rec.source_entity
                    );

                    v_row_count := v_row_count + 1;
                END IF;

            ELSE
                -- First time insert for this source_id
                INSERT INTO bl_3nf.CE_CUSTOMERS_SCD (
                    customer_id,
                    customer_src_id,
                    customer_name,
                    customer_email,
                    customer_phone,
                    customer_gender,
                    customer_zip_code_id,
                    customer_dob,
                    customer_registration_dt,
                    insert_dt,
                    start_dt,
                    end_dt,
                    is_active,
                    source_system,
                    source_entity
                ) VALUES (
                    rec.customer_id,
                    rec.customer_src_id,
                    rec.customer_name,
                    rec.customer_email,
                    rec.customer_phone,
                    rec.customer_gender,
                    v_zip_id,
                    rec.customer_dob,
                    rec.customer_registration_dt,
                    rec.start_dt,
                    rec.start_dt,
                    DATE '9999-12-31',
                    TRUE,
                    rec.source_system,
                    rec.source_entity
                );

                v_row_count := v_row_count + 1;
            END IF;

        EXCEPTION WHEN OTHERS THEN
            CALL bl_cl.p_log_etl(
                'p_load_ce_customers_scd',
                0,
                'Error on customer_src_id = ' || rec.customer_src_id || ': ' || SQLERRM,
                'ERROR'
            );
        END;
    END LOOP;

    -- Log success
    CALL bl_cl.p_log_etl(
        'p_load_ce_customers_scd',
        v_row_count,
        'SCD customers loaded successfully.',
        'INFO'
    );
END;
$$;




CALL bl_cl.p_load_ce_customers_scd();

SELECT * FROM bl_3nf.CE_CUSTOMERS_SCD csd  where csd.customer_src_id ='EDNA_036';
ORDER BY customer_id, start_dt;

select * from bl_cl.lkp_customers lc where lc.customer_src_id ='EDNA_036';

--1st run:same data
CALL bl_cl.p_load_ce_employees();
select * from bl_3nf.etl_logs el where el.procedure_name ='p_load_ce_employees';
SELECT * FROM bl_3nf.CE_EMPLOYEES
ORDER BY employee_id;

--2nd run:same data 
CALL bl_cl.p_load_ce_employees();
select * from bl_3nf.etl_logs;
SELECT * FROM bl_3nf.CE_EMPLOYEES
ORDER BY employee_id;

--3rd run:1 additional entry
CALL bl_cl.p_load_ce_employees();
select * from bl_3nf.etl_logs el where el.procedure_name ='p_load_ce_employees';
SELECT * FROM bl_3nf.CE_EMPLOYEES
ORDER BY employee_id;


-- ======================================
-- Sales
-- ======================================
CREATE OR REPLACE FUNCTION bl_cl.f_get_merged_sales()
RETURNS TABLE (
    game_number_src_id VARCHAR,
    customer_src_id VARCHAR,
    employee_src_id VARCHAR,
    retailer_license_number_src_id VARCHAR,
    payment_method_src_id VARCHAR,
    event_dt TIMESTAMP,
    tickets_bought INTEGER,
    payout NUMERIC,
    sales NUMERIC,
    ticket_price NUMERIC,
    insert_dt TIMESTAMP,
    update_dt TIMESTAMP,
    source_system VARCHAR,
    source_entity VARCHAR
)
LANGUAGE sql
AS $$
    SELECT
        game_number_src_id,
        customer_src_id,
        employee_src_id,
        retailer_license_number_src_id,
        payment_method_src_id,
        event_dt,
        tickets_bought,
        payout,
        sales,
        ticket_price,
        insert_dt,
        update_dt,
        source_system,
        source_entity
    FROM bl_cl.lkp_sales
$$;


CREATE OR REPLACE PROCEDURE bl_cl.p_load_ce_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_game_number_id BIGINT;
    v_customer_id BIGINT;
    v_employee_id BIGINT;
    v_retailer_license_number_id BIGINT;
    v_payment_id BIGINT;
    v_row_count INT := 0;
    v_inserted INT;
BEGIN
    FOR rec IN SELECT * FROM bl_cl.f_get_merged_sales()
    LOOP
        BEGIN
            -- Resolve GAME_NUMBER_ID
            SELECT game_number_id INTO v_game_number_id
            FROM bl_3nf.CE_GAME_NUMBERS
            WHERE game_number_src_id = rec.game_number_src_id
            LIMIT 1;
            IF v_game_number_id IS NULL THEN v_game_number_id := -1; END IF;

            -- Resolve CUSTOMER_ID (latest active SCD record)
            SELECT customer_id INTO v_customer_id
            FROM bl_3nf.CE_CUSTOMERS_SCD
            WHERE customer_src_id = rec.customer_src_id
            ORDER BY start_dt DESC
            LIMIT 1;
            IF v_customer_id IS NULL THEN v_customer_id := -1; END IF;

            -- Resolve EMPLOYEE_ID
            SELECT employee_id INTO v_employee_id
            FROM bl_3nf.CE_EMPLOYEES
            WHERE employee_src_id = rec.employee_src_id
            LIMIT 1;
            IF v_employee_id IS NULL THEN v_employee_id := -1; END IF;

            -- Resolve RETAILER_LICENSE_NUMBER_ID
            SELECT retailer_license_number_id INTO v_retailer_license_number_id
            FROM bl_3nf.CE_RETAILER_LICENSE_NUMBERS
            WHERE retailer_license_number_src_id = rec.retailer_license_number_src_id
            LIMIT 1;
            IF v_retailer_license_number_id IS NULL THEN v_retailer_license_number_id := -1; END IF;

            -- Resolve PAYMENT_ID
            SELECT payment_method_id INTO v_payment_id
            FROM bl_3nf.CE_PAYMENT_METHODS
            WHERE payment_method_src_id = rec.payment_method_src_id
            LIMIT 1;
            IF v_payment_id IS NULL THEN v_payment_id := -1; END IF;

            -- Insert into CE_SALES
            INSERT INTO bl_3nf.CE_SALES (
                GAME_NUMBER_ID,
                CUSTOMER_ID,
                EMPLOYEE_ID,
                RETAILER_LICENSE_NUMBER_ID,
                PAYMENT_ID,
                EVENT_DT,
                TICKETS_BOUGHT,
                PAYOUT,
                SALES,
                TICKET_PRICE,
                INSERT_DT,
                UPDATE_DT
            )
            VALUES (
                v_game_number_id,
                v_customer_id,
                v_employee_id,
                v_retailer_license_number_id,
                v_payment_id,
                rec.event_dt,
                rec.tickets_bought,
                rec.payout,
                rec.sales,
                rec.ticket_price,
                rec.insert_dt,
                rec.update_dt
            )
            ON CONFLICT DO NOTHING;

            GET DIAGNOSTICS v_inserted = ROW_COUNT;
            v_row_count := v_row_count + v_inserted;

        EXCEPTION WHEN OTHERS THEN
            CALL bl_cl.p_log_etl(
                'p_load_ce_sales',
                0,
                'Error processing sales record (event_dt=' || rec.event_dt || ', customer_src_id=' || rec.customer_src_id || '): ' || SQLERRM,
                'ERROR'
            );
        END;
    END LOOP;

    CALL bl_cl.p_log_etl(
        'p_load_ce_sales',
        v_row_count,
        'Successfully loaded sales records.',
        'INFO'
    );
END;
$$;



