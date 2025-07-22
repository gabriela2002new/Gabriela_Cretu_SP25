-- ======================================
-- Game Types
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_game_types()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_game_types;

    INSERT INTO bl_cl.mta_game_types (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('game_type_id',     'n. a.',      'INT',     'Generated via sequence',               FALSE, 'Surrogate key'),
        ('game_type_src_id', 'game_type',  'VARCHAR', 'COALESCE(game_type::VARCHAR, ''n. a.'')', FALSE, 'Source identifier'),
        ('game_type_name',   'game_type',  'VARCHAR', 'Same as src_id',                       FALSE, 'Used as display name'),
        ('source_system',    'n. a.',      'VARCHAR', 'Static: source system name',           FALSE, 'E.g., sa_final_scratch'),
        ('source_entity',    'n. a.',      'VARCHAR', 'Static: source entity name',           FALSE, 'E.g., src_final_scratch'),
        ('insert_dt',        'n. a.',      'DATE',    'CURRENT_DATE',                         FALSE, 'Load timestamp'),
        ('update_dt',        'n. a.',      'DATE',    'CURRENT_DATE',                         FALSE, 'Last modified');

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_game_types',
        v_rows,
        'Successfully inserted metadata for game types.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_insert_meta_game_types',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;



CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_game_types()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_game_types;

    INSERT INTO bl_cl.wrk_game_types (
        game_type_src_id,
        game_type_name,
        source_system,
        source_entity,
        load_dt
    )
    SELECT DISTINCT
        COALESCE(game_type::VARCHAR, 'n. a.'),
        COALESCE(game_type::VARCHAR, 'n. a.'),
        'sa_final_scratch',
        'src_final_scratch',
        CURRENT_DATE
    FROM sa_final_scratch.src_final_scratch
    WHERE game_type IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        COALESCE(game_type::VARCHAR, 'n. a.'),
        COALESCE(game_type::VARCHAR, 'n. a.'),
        'sa_final_draw',
        'src_final_draw',
        CURRENT_DATE
    FROM sa_final_draw.src_final_draw
    WHERE game_type IS NOT NULL;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_game_types',
        v_rows,
        'Successfully loaded working game types.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_load_wrk_game_types',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

drop sequence if exists bl_cl.t_map_game_types_seq;
CREATE SEQUENCE IF NOT EXISTS bl_cl.t_map_game_types_seq START WITH 1 INCREMENT BY 1;


CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_game_types()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_types AS (
        SELECT DISTINCT
            w.game_type_src_id,
            w.game_type_name,
            w.source_system,
            w.source_entity
        FROM bl_cl.wrk_game_types w
    ),
    surrogate_keys AS (
        SELECT 
            game_type_src_id,
            nextval('bl_cl.t_map_game_types_seq') AS game_type_id
        FROM (
            SELECT DISTINCT game_type_src_id FROM new_types
        ) s
    )
    INSERT INTO bl_cl.lkp_game_types (
        game_type_id,
        game_type_src_id,
        game_type_name,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT
        sk.game_type_id,
        nt.game_type_src_id,
        nt.game_type_name,
        nt.source_system,
        nt.source_entity,
        CURRENT_DATE,
        CURRENT_DATE
    FROM new_types nt
    JOIN surrogate_keys sk ON nt.game_type_src_id = sk.game_type_src_id
    ON CONFLICT (game_type_src_id, source_system, source_entity)
    DO UPDATE SET
        game_type_name = EXCLUDED.game_type_name,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_game_types',
        v_rows,
        'Successfully upserted game types into lookup table.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_upsert_lkp_game_types',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;


CALL bl_cl.sp_insert_meta_game_types();
select* from bl_cl.mta_game_types mgt ;
CALL bl_cl.sp_load_wrk_game_types();
select * from bl_cl.wrk_game_types wkt;
CALL bl_cl.sp_upsert_lkp_game_types();
select * from bl_cl.lkp_game_types lgt;

select* from bl_cl.mta_game_types mgt ;
select * from bl_cl.wrk_game_types wkt;
select * from bl_cl.lkp_game_types lgt;

select * from bl_3nf.etl_logs;

-- ======================================
-- Game Categories
-- ======================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_game_categories()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_game_categories;

    INSERT INTO bl_cl.mta_game_categories (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('game_category_id',     'n. a.',         'BIGINT', 'Generated via sequence',          FALSE, 'Surrogate key'),
        ('game_category_src_id', 'game_category', 'VARCHAR','COALESCE(game_category, ''n. a.'')', FALSE, 'Raw identifier from source'),
        ('game_category_name',   'game_category', 'VARCHAR','Same as src_id',                 FALSE, 'Name for display'),
        ('game_type_name',       'game_type',     'VARCHAR','COALESCE(game_type, ''n. a.'')', FALSE, 'Linked game type'),
        ('winning_chance',       'winning_chance','FLOAT',  'COALESCE(winning_chance, -1)',   TRUE,  'Chance of winning'),
        ('winning_jackpot',      'winning_jackpot','FLOAT', 'COALESCE(winning_jackpot, -1)',  TRUE,  'Jackpot value'),
        ('source_system',        'n. a.',         'VARCHAR','Static',                         FALSE, 'E.g., sa_final_draw'),
        ('source_entity',        'n. a.',         'VARCHAR','Static',                         FALSE, 'E.g., src_final_draw'),
        ('insert_dt',            'n. a.',         'DATE',   'CURRENT_DATE',                   FALSE, 'Insert timestamp'),
        ('update_dt',            'n. a.',         'DATE',   'CURRENT_DATE',                   FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_game_categories',
        v_rows,
        'Successfully inserted metadata for game categories.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_insert_meta_game_categories',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_game_categories()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_game_categories;

    INSERT INTO bl_cl.wrk_game_categories (
        game_category_src_id,
        game_type_name,
        winning_chance,
        winning_jackpot,
        source_system,
        source_entity,
        load_dt
    )
    SELECT DISTINCT
        COALESCE(s.game_category::VARCHAR, 'n. a.'),
        COALESCE(s.game_type, 'n. a.'),
        NULL::FLOAT,
        NULL::FLOAT,
        'sa_final_scratch',
        'src_final_scratch',
        CURRENT_DATE
    FROM sa_final_scratch.src_final_scratch s
    WHERE s.game_category IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        COALESCE(d.game_category::VARCHAR, 'n. a.'),
        COALESCE(d.game_type, 'n. a.'),
        COALESCE(d.winning_chance::FLOAT, -1),
        COALESCE(d.winning_jackpot::FLOAT, -1),
        'sa_final_draw',
        'src_final_draw',
        CURRENT_DATE
    FROM sa_final_draw.src_final_draw d
    WHERE d.game_category IS NOT NULL;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_game_categories',
        v_rows,
        'Successfully loaded working game categories.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_load_wrk_game_categories',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_game_categories()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_categories AS (
        SELECT DISTINCT
            w.game_category_src_id,
            w.game_type_name,
            w.winning_chance,
            w.winning_jackpot,
            w.source_system,
            w.source_entity
        FROM bl_cl.wrk_game_categories w
    ),
    surrogate_keys AS (
        SELECT 
            game_category_src_id,
            source_system,
            source_entity,
            nextval('bl_cl.ce_game_categories_seq') AS game_category_id
        FROM (
            SELECT DISTINCT game_category_src_id, source_system, source_entity
            FROM new_categories
        ) s
    )
    INSERT INTO bl_cl.lkp_game_categories (
        game_category_id,
        game_category_src_id,
        game_category_name,
        game_type_name,
        winning_chance,
        winning_jackpot,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT
        sk.game_category_id,
        nc.game_category_src_id,
        nc.game_category_src_id,  -- Name same as src_id
        nc.game_type_name,
        COALESCE(nc.winning_chance, -1),
        COALESCE(nc.winning_jackpot, -1),
        nc.source_system,
        nc.source_entity,
        CURRENT_DATE,
        CURRENT_DATE
    FROM new_categories nc
    JOIN surrogate_keys sk 
      ON nc.game_category_src_id = sk.game_category_src_id
     AND nc.source_system = sk.source_system
     AND nc.source_entity = sk.source_entity
    ON CONFLICT (game_category_src_id, source_system, source_entity)
    DO UPDATE SET
        game_type_name = EXCLUDED.game_type_name,
        winning_chance = EXCLUDED.winning_chance,
        winning_jackpot = EXCLUDED.winning_jackpot,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_game_categories',
        v_rows,
        'Successfully upserted game categories into lookup table.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_upsert_lkp_game_categories',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CALL bl_cl.sp_insert_meta_game_categories();
CALL bl_cl.sp_load_wrk_game_categories();
CALL bl_cl.sp_upsert_lkp_game_categories();

select* from bl_cl.mta_game_categories mgc ;
select * from bl_cl.wrk_game_categories wgc;
select * from bl_cl.lkp_game_categories lgc;
-- ======================================
-- Game Numbers 
-- ======================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_game_numbers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_game_numbers;

    INSERT INTO bl_cl.mta_game_numbers (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('game_number_id',       'n. a.',           'BIGINT',  'Generated via sequence',              FALSE, 'Surrogate key'),
        ('game_number_src_id',   'game_number',     'VARCHAR', 'Raw identifier from source',          FALSE, 'Source raw ID'),
        ('game_number_name',     'game_number',     'VARCHAR', 'Same as src_id',                      FALSE, 'Name for display'),
        ('game_category_name',   'game_category',   'VARCHAR', 'COALESCE(game_category, ''n. a.'')',  TRUE,  'Category name'),
        ('draw_dt',              'draw_dt',         'DATE',    'COALESCE(draw_dt, DATE ''1900-01-01'')', TRUE, 'Draw date or default'),
        ('average_odds',         'average_odds',    'VARCHAR', 'COALESCE(average_odds, ''n. a.'')',    TRUE,  'Average odds as string'),
        ('average_odds_prob',    'average_odds_prob','FLOAT',  'COALESCE(average_odds_prob, -1)',      TRUE,  'Average odds as probability'),
        ('mid_tier_prize',       'mid_tier_prize',  'FLOAT',   'COALESCE(mid_tier_prize, -1)',         TRUE,  'Mid tier prize'),
        ('top_tier_prize',       'top_tier_prize',  'FLOAT',   'COALESCE(top_tier_prize, -1)',         TRUE,  'Top tier prize'),
        ('small_prize',          'small_prize',     'FLOAT',   'COALESCE(small_prize, -1)',            TRUE,  'Small prize'),
        ('source_system',        'n. a.',           'VARCHAR', 'Static',                               FALSE, 'E.g., sa_final_draw'),
        ('source_entity',        'n. a.',           'VARCHAR', 'Static',                               FALSE, 'E.g., src_final_draw'),
        ('insert_dt',            'n. a.',           'DATE',    'CURRENT_DATE',                         FALSE, 'Insert timestamp'),
        ('update_dt',            'n. a.',           'DATE',    'CURRENT_DATE',                         FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_game_numbers',
        v_rows,
        'Successfully inserted metadata for game numbers.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_insert_meta_game_numbers',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_game_numbers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_game_numbers;

    INSERT INTO bl_cl.wrk_game_numbers (
        game_number_src_id,
        game_category_name,
        draw_dt,
        average_odds,
        average_odds_prob,
        mid_tier_prize,
        top_tier_prize,
        small_prize,
        source_system,
        source_entity,
        load_dt
    )
    SELECT DISTINCT
        COALESCE(s.game_number::VARCHAR, 'n. a.'),
        COALESCE(s.game_category, 'n. a.'),
        DATE '1900-01-01',
        COALESCE(CAST(s.average_odds AS VARCHAR(30)), 'n. a.'),
        COALESCE(CAST(s.average_odds_prob AS FLOAT), -1),
        COALESCE(CAST(s.mid_prize AS FLOAT), -1),
        COALESCE(CAST(s.top_prize AS FLOAT), -1),
        COALESCE(CAST(s.small_prize AS FLOAT), -1),
        'sa_final_scratch',
        'src_final_scratch',
        CURRENT_DATE
    FROM sa_final_scratch.src_final_scratch s
    WHERE s.game_number IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        COALESCE(d.game_number::VARCHAR, 'n. a.'),
        COALESCE(d.game_category, 'n. a.'),
        COALESCE(d.draw_dt::DATE, DATE '1900-01-01'),
        'n. a.',
        -1,
        -1,
        -1,
        -1,
        'sa_final_draw',
        'src_final_draw',
        CURRENT_DATE
    FROM sa_final_draw.src_final_draw d
    WHERE d.game_number IS NOT NULL;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_game_numbers',
        v_rows,
        'Successfully loaded working game numbers.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_load_wrk_game_numbers',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_game_numbers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_numbers AS (
        SELECT DISTINCT
            w.game_number_src_id,
            w.game_category_name,
            w.draw_dt,
            w.average_odds,
            w.average_odds_prob,
            w.mid_tier_prize,
            w.top_tier_prize,
            w.small_prize,
            w.source_system,
            w.source_entity
        FROM bl_cl.wrk_game_numbers w
    ),
    surrogate_keys AS (
        SELECT
            game_number_src_id,
            nextval('bl_cl.ce_game_numbers_seq') AS game_number_id
        FROM (
            SELECT DISTINCT game_number_src_id FROM new_numbers
        ) s
    )
    INSERT INTO bl_cl.lkp_game_numbers (
        game_number_id,
        game_number_src_id,
        game_number_name,
        game_category_name,
        draw_dt,
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
    SELECT
        sk.game_number_id,
        nn.game_number_src_id,
        nn.game_number_src_id,
        nn.game_category_name,
        nn.draw_dt,
        nn.average_odds,
        nn.average_odds_prob,
        nn.mid_tier_prize,
        nn.top_tier_prize,
        nn.small_prize,
        nn.source_system,
        nn.source_entity,
        CURRENT_DATE,
        CURRENT_DATE
    FROM new_numbers nn
    JOIN surrogate_keys sk ON nn.game_number_src_id = sk.game_number_src_id
    ON CONFLICT (game_number_src_id, source_system, source_entity)
    DO UPDATE SET
        game_category_name = EXCLUDED.game_category_name,
        draw_dt = EXCLUDED.draw_dt,
        average_odds = EXCLUDED.average_odds,
        average_odds_prob = EXCLUDED.average_odds_prob,
        mid_tier_prize = EXCLUDED.mid_tier_prize,
        top_tier_prize = EXCLUDED.top_tier_prize,
        small_prize = EXCLUDED.small_prize,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_game_numbers',
        v_rows,
        'Successfully upserted game numbers to lookup table.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_upsert_lkp_game_numbers',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;


CALL bl_cl.sp_insert_meta_game_numbers();
CALL bl_cl.sp_load_wrk_game_numbers();
CALL bl_cl.sp_upsert_lkp_game_numbers();

select* from bl_cl.mta_game_numbers mgn  ;
select * from bl_cl.wrk_game_numbers wgn ;
select * from bl_cl.lkp_game_numbers lgn order by game_number_id ;

-- ======================================
-- Payment Methods
-- ======================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_payment_methods()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_payment_methods;

    INSERT INTO bl_cl.mta_payment_methods (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('payment_method_id',      'n. a.',               'BIGINT',  'Generated via sequence',                FALSE, 'Surrogate key'),
        ('payment_method_src_id',  'payment_method_id',   'VARCHAR', 'Raw identifier from source',            FALSE, 'Source raw ID'),
        ('payment_method_name',    'payment_method_name', 'VARCHAR', 'COALESCE(payment_method_name, ''n.a.'')', TRUE,  'Human-readable name'),
        ('source_system',          'n. a.',               'VARCHAR', 'Static',                               FALSE, 'e.g., sa_final_draw'),
        ('source_entity',          'n. a.',               'VARCHAR', 'Static',                               FALSE, 'e.g., src_final_draw'),
        ('insert_dt',              'n. a.',               'DATE',    'CURRENT_DATE',                         FALSE, 'Insert timestamp'),
        ('update_dt',              'n. a.',               'DATE',    'CURRENT_DATE',                         FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_payment_methods',
        v_rows,
        'Successfully inserted metadata for payment methods.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_insert_meta_payment_methods',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_payment_methods()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_payment_methods;

    INSERT INTO bl_cl.wrk_payment_methods (
        payment_method_src_id,
        payment_method_name,
        source_system,
        source_entity,
        load_dt
    )
    SELECT DISTINCT
        COALESCE(payment_method_id::VARCHAR, 'n.a.'),
        COALESCE(payment_method_name::VARCHAR, 'n.a.'),
        'sa_final_scratch',
        'src_final_scratch',
        CURRENT_DATE
    FROM sa_final_scratch.src_final_scratch
    WHERE payment_method_id IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        COALESCE(payment_method_id::VARCHAR, 'n.a.'),
        COALESCE(payment_method_name::VARCHAR, 'n.a.'),
        'sa_final_draw',
        'src_final_draw',
        CURRENT_DATE
    FROM sa_final_draw.src_final_draw
    WHERE payment_method_id IS NOT NULL;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_payment_methods',
        v_rows,
        'Successfully loaded working payment methods.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_load_wrk_payment_methods',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_payment_methods()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_payment_methods AS (
        SELECT DISTINCT
            w.payment_method_src_id,
            w.payment_method_name,
            w.source_system,
            w.source_entity
        FROM bl_cl.wrk_payment_methods w
    ),
    surrogate_keys AS (
        SELECT 
            payment_method_src_id,
            nextval('bl_cl.ce_payment_methods_seq') AS payment_method_id
        FROM (
            SELECT DISTINCT payment_method_src_id
            FROM new_payment_methods
        ) s
    )
    INSERT INTO bl_cl.lkp_payment_methods (
        payment_method_id,
        payment_method_src_id,
        payment_method_name,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT
        sk.payment_method_id,
        npm.payment_method_src_id,
        npm.payment_method_name,
        npm.source_system,
        npm.source_entity,
        CURRENT_DATE,
        CURRENT_DATE
    FROM new_payment_methods npm
    JOIN surrogate_keys sk 
      ON npm.payment_method_src_id = sk.payment_method_src_id
    ON CONFLICT (payment_method_src_id, source_system, source_entity)
    DO UPDATE SET
        payment_method_name = EXCLUDED.payment_method_name,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_payment_methods',
        v_rows,
        'Successfully upserted payment methods into lookup table.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_upsert_lkp_payment_methods',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CALL bl_cl.sp_insert_meta_payment_methods();
CALL bl_cl.sp_load_wrk_payment_methods();
CALL bl_cl.sp_upsert_lkp_payment_methods();

select* from bl_cl.mta_payment_methods mpm  ;
select * from bl_cl.wrk_payment_methods wpm order by payment_method_src_id ;
select * from bl_cl.lkp_payment_methods lpm order by payment_method_src_id;
-- ======================================
-- States
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_states()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_states;

    INSERT INTO bl_cl.mta_states (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('state_id',      'n.a.',                         'BIGINT',  'Generated via sequence',               FALSE, 'Surrogate key'),
        ('state_src_id',  'retailer_location_state OR customer_state', 'VARCHAR',  'COALESCE(field, ''n. a.'')', FALSE, 'Raw identifier from source'),
        ('state_name',    'same as src_id',               'VARCHAR',  'Same as src_id',                       TRUE,  'Human-readable name (alias)'),
        ('source_system', 'n.a.',                         'VARCHAR',  'Static',                               FALSE, 'e.g., sa_final_draw'),
        ('source_entity', 'n.a.',                         'VARCHAR',  'Static',                               FALSE, 'e.g., src_final_draw'),
        ('insert_dt',     'n.a.',                         'DATE',     'CURRENT_DATE',                         FALSE, 'Insert timestamp'),
        ('update_dt',     'n.a.',                         'DATE',     'CURRENT_DATE',                         FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_states',
        v_rows,
        'Successfully inserted metadata for states.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_insert_meta_states',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_states()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_states;

    INSERT INTO bl_cl.wrk_states (
        state_src_id,
        source_system,
        source_entity,
        load_dt
    )
    SELECT DISTINCT
        COALESCE(retailer_location_state::VARCHAR, 'n. a.'),
        'sa_final_draw',
        'src_final_draw',
        CURRENT_DATE
    FROM sa_final_draw.src_final_draw
    WHERE retailer_location_state IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        COALESCE(customer_state::VARCHAR, 'n. a.'),
        'sa_final_draw',
        'src_final_draw',
        CURRENT_DATE
    FROM sa_final_draw.src_final_draw
    WHERE customer_state IS NOT NULL;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_states',
        v_rows,
        'Successfully loaded working states.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_load_wrk_states',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_states()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_states AS (
        SELECT DISTINCT
            w.state_src_id,
            w.source_system,
            w.source_entity
        FROM bl_cl.wrk_states w
    ),
    surrogate_keys AS (
        SELECT 
            state_src_id,
            nextval('bl_cl.ce_states_seq') AS state_id
        FROM (
            SELECT DISTINCT state_src_id
            FROM new_states
        ) s
    )
    INSERT INTO bl_cl.lkp_states (
        state_id,
        state_src_id,
        state_name,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT
        sk.state_id,
        ns.state_src_id,
        ns.state_src_id,  -- Used as name
        ns.source_system,
        ns.source_entity,
        CURRENT_DATE,
        CURRENT_DATE
    FROM new_states ns
    JOIN surrogate_keys sk 
      ON ns.state_src_id = sk.state_src_id
    ON CONFLICT (state_src_id, source_system, source_entity)
    DO UPDATE SET
        state_name = EXCLUDED.state_name,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_states',
        v_rows,
        'Successfully upserted states into lookup table.',
        'INFO'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_3nf.p_log_etl(
            'sp_upsert_lkp_states',
            0,
            'Error: ' || SQLERRM,
            'ERROR'
        );
        RAISE;
END;
$$;

CALL bl_cl.sp_insert_meta_states();
CALL bl_cl.sp_load_wrk_states();
CALL bl_cl.sp_upsert_lkp_states();
select* from bl_cl.mta_states ms ;
select * from bl_cl.wrk_states ws ;
select * from bl_cl.lkp_states ls ;
-- ======================================
-- Cities
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_cities()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_cities;

    INSERT INTO bl_cl.mta_cities (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('city_id',       'n.a.',                                    'BIGINT',   'Generated via sequence',     FALSE, 'Surrogate key'),
        ('city_src_id',   'customer_city OR retailer_location_city', 'VARCHAR',  'COALESCE(field, ''n. a.'')', FALSE, 'City identifier from source'),
        ('state_name',    'customer_state or retailer_location_state','VARCHAR', 'Derived by join',             TRUE,  'Linked state name'),
        ('city_name',     'same as src_id',                          'VARCHAR',  'Same as city_src_id',         TRUE,  'City name'),
        ('source_system', 'n.a.',                                    'VARCHAR',  'Static value',                FALSE, 'e.g. sa_final_draw'),
        ('source_entity', 'n.a.',                                    'VARCHAR',  'Static value',                FALSE, 'e.g. src_final_draw'),
        ('insert_dt',     'n.a.',                                    'DATE',     'CURRENT_DATE',                FALSE, 'Insert timestamp'),
        ('update_dt',     'n.a.',                                    'DATE',     'CURRENT_DATE',                FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_cities',
        v_rows,
        'Successfully inserted metadata for cities.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_cities',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_cities()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_cities;

    WITH
    draw_customer_cities AS (
        SELECT DISTINCT
            COALESCE(customer_city::VARCHAR, 'n. a.') AS city_src_id,
            'sa_final_draw' AS source_system,
            'src_final_draw' AS source_entity,
            customer_id
        FROM sa_final_draw.src_final_draw
    ),
    scratch_retailer_cities AS (
        SELECT DISTINCT
            COALESCE(retailer_location_city::VARCHAR, 'n. a.') AS city_src_id,
            'sa_final_scratch' AS source_system,
            'src_final_scratch' AS source_entity,
            retailer_license_number
        FROM sa_final_scratch.src_final_scratch
    ),
    draw_customer_states AS (
        SELECT DISTINCT
            customer_id,
            COALESCE(customer_state::VARCHAR, 'n. a.') AS state_name
        FROM sa_final_draw.src_final_draw
    ),
    draw_retailer_states AS (
        SELECT DISTINCT
            retailer_license_number,
            COALESCE(retailer_location_state::VARCHAR, 'n. a.') AS state_name
        FROM sa_final_draw.src_final_draw
    ),
    customer_city_state AS (
        SELECT DISTINCT
            c.city_src_id,
            COALESCE(s.state_name, 'n. a.') AS state_name,
            c.source_system,
            c.source_entity
        FROM draw_customer_cities c
        LEFT JOIN draw_customer_states s ON c.customer_id = s.customer_id
    ),
    retailer_city_state AS (
        SELECT DISTINCT
            r.city_src_id,
            COALESCE(s.state_name, 'n. a.') AS state_name,
            r.source_system,
            r.source_entity
        FROM scratch_retailer_cities r
        LEFT JOIN draw_retailer_states s ON r.retailer_license_number = s.retailer_license_number
    ),
    all_city_states AS (
        SELECT * FROM customer_city_state
        UNION ALL
        SELECT * FROM retailer_city_state
    )
    INSERT INTO bl_cl.wrk_cities (
        city_src_id,
        state_name,
        source_system,
        source_entity,
        load_dt
    )
    SELECT
        city_src_id,
        state_name,
        source_system,
        source_entity,
        CURRENT_DATE
    FROM all_city_states;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_cities',
        v_rows,
        'Successfully loaded working cities.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_cities',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_cities()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_city_candidates AS (
        SELECT DISTINCT
            w.city_src_id,
            w.state_name,
            w.source_system,
            w.source_entity
        FROM bl_cl.wrk_cities w
    ),
    surrogate_keys AS (
        SELECT 
            city_src_id,
            nextval('bl_cl.ce_cities_seq') AS city_id
        FROM (
            SELECT DISTINCT city_src_id
            FROM new_city_candidates
        ) s
    ),
    cities_with_ids AS (
        SELECT
            sk.city_id,
            nc.city_src_id,
            COALESCE(nc.state_name, 'n. a.') AS state_name,
            nc.source_system,
            nc.source_entity
        FROM new_city_candidates nc
        LEFT JOIN surrogate_keys sk 
          ON nc.city_src_id = sk.city_src_id
    ),
    aggregated_cities AS (
        SELECT
            city_id,
            city_src_id,
            COALESCE(
                MAX(CASE WHEN state_name <> 'n. a.' THEN state_name END),
                'n. a.'
            ) AS state_name,
            source_system,
            source_entity
        FROM cities_with_ids
        GROUP BY city_id, city_src_id, source_system, source_entity
    )
    INSERT INTO bl_cl.lkp_cities (
        city_id,
        city_src_id,
        state_name,
        city_name,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT
        city_id,
        city_src_id,
        state_name,
        city_src_id,
        source_system,
        source_entity,
        CURRENT_DATE,
        CURRENT_DATE
    FROM aggregated_cities
    ON CONFLICT (city_src_id, source_system, source_entity)
    DO UPDATE SET
        state_name = EXCLUDED.state_name,
        city_name  = EXCLUDED.city_name,
        update_dt  = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_cities',
        v_rows,
        'Successfully upserted cities into lookup table.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_cities',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;


CALL bl_cl.sp_insert_meta_cities();
CALL bl_cl.sp_load_wrk_cities() ;
CALL bl_cl.sp_upsert_lkp_cities();

select* from bl_cl.mta_cities mc  ;
select * from bl_cl.wrk_cities wc order by city_src_id ; 
select * from bl_cl.lkp_cities lc order by city_src_id;

 -- ======================================
-- Zipcodes
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_zips()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_zips;

    INSERT INTO bl_cl.mta_zips (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('zip_id',        'n.a.',                                  'BIGINT',  'Generated via sequence',         FALSE, 'Surrogate key'),
        ('zip_src_id',    'customer_zip_code or retailer_zip_code','VARCHAR', 'COALESCE(field, ''n. a.'')',     FALSE, 'ZIP code from source'),
        ('city_name',     'customer_city or retailer_location_city','VARCHAR', 'COALESCE(field, ''n. a.'')',     TRUE,  'Linked city name'),
        ('zip_name',      'same as zip_src_id',                    'VARCHAR', 'Same as zip_src_id',             FALSE, 'Redundant zip name'),
        ('source_system', 'n.a.',                                  'VARCHAR', 'Static value',                   FALSE, 'e.g. sa_final_draw'),
        ('source_entity', 'n.a.',                                  'VARCHAR', 'Static value',                   FALSE, 'e.g. src_final_draw'),
        ('insert_dt',     'n.a.',                                  'DATE',    'CURRENT_DATE',                   FALSE, 'Insert timestamp'),
        ('update_dt',     'n.a.',                                  'DATE',    'CURRENT_DATE',                   FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_zips',
        v_rows,
        'Inserted metadata rows into mta_zips.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_zips',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_zips()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_zips;

    WITH
    retailer_zip_city AS (
        SELECT DISTINCT
            COALESCE(retailer_location_zip_code::VARCHAR, 'n. a.') AS zip_src_id,
            COALESCE(retailer_location_city::VARCHAR, 'n. a.') AS city_name
        FROM sa_final_scratch.src_final_scratch
    ),
    customer_zip_city_raw AS (
        SELECT DISTINCT
            COALESCE(customer_zip_code::VARCHAR, 'n. a.') AS zip_src_id,
            COALESCE(customer_city::VARCHAR, 'n. a.') AS city_name,
            'sa_final_draw' AS source_system,
            'src_final_draw' AS source_entity
        FROM sa_final_draw.src_final_draw
    ),
    cleaned_customer_zip_city AS (
        SELECT
            c.zip_src_id,
            COALESCE(r.city_name, c.city_name) AS city_name,
            c.source_system,
            c.source_entity
        FROM customer_zip_city_raw c
        LEFT JOIN retailer_zip_city r
            ON c.zip_src_id = r.zip_src_id
    ),
    retailer_zip_city_full AS (
        SELECT DISTINCT
            COALESCE(retailer_location_zip_code::VARCHAR, 'n. a.') AS zip_src_id,
            COALESCE(retailer_location_city::VARCHAR, 'n. a.') AS city_name,
            'sa_final_scratch' AS source_system,
            'src_final_scratch' AS source_entity
        FROM sa_final_scratch.src_final_scratch
    ),
    combined_zip_city AS (
        SELECT * FROM cleaned_customer_zip_city
        UNION ALL
        SELECT * FROM retailer_zip_city_full
    ),
    final_zip_city AS (
        SELECT DISTINCT ON (zip_src_id, source_system, source_entity)
            zip_src_id,
            city_name,
            source_system,
            source_entity
        FROM combined_zip_city
        ORDER BY zip_src_id, source_system, source_entity
    )
    INSERT INTO bl_cl.wrk_zips (
        zip_src_id,
        city_name,
        source_system,
        source_entity,
        load_dt
    )
    SELECT
        zip_src_id,
        city_name,
        source_system,
        source_entity,
        CURRENT_DATE
    FROM final_zip_city;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_zips',
        v_rows,
        'Loaded working zips successfully.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_zips',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_zips()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_zip_candidates AS (
        SELECT DISTINCT
            zip_src_id,
            city_name,
            source_system,
            source_entity
        FROM bl_cl.wrk_zips
    ),
    surrogate_keys AS (
        SELECT 
            zip_src_id,
            nextval('bl_cl.ce_zips_seq') AS zip_id
        FROM (
            SELECT DISTINCT zip_src_id
            FROM new_zip_candidates
        ) s
    ),
    zips_with_ids AS (
        SELECT
            sk.zip_id,
            nc.zip_src_id,
            COALESCE(nc.city_name, 'n. a.') AS city_name,
            nc.source_system,
            nc.source_entity
        FROM new_zip_candidates nc
        LEFT JOIN surrogate_keys sk 
          ON nc.zip_src_id = sk.zip_src_id
    ),
    aggregated_zips AS (
        SELECT
            zip_id,
            zip_src_id,
            COALESCE(
                MAX(CASE WHEN city_name <> 'n. a.' THEN city_name END),
                'n. a.'
            ) AS city_name,
            source_system,
            source_entity
        FROM zips_with_ids
        GROUP BY zip_id, zip_src_id, source_system, source_entity
    )
    INSERT INTO bl_cl.lkp_zips (
        zip_id,
        zip_src_id,
        city_name,
        zip_name,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT
        zip_id,
        zip_src_id,
        city_name,
        zip_src_id,
        source_system,
        source_entity,
        CURRENT_DATE,
        CURRENT_DATE
    FROM aggregated_zips
    ON CONFLICT (zip_src_id, source_system, source_entity) DO UPDATE
    SET
        city_name = EXCLUDED.city_name,
        zip_name  = EXCLUDED.zip_name,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_zips',
        v_rows,
        'Upserted lookup zips successfully.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_zips',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;

CALL bl_cl.sp_insert_meta_zips();
CALL bl_cl.sp_load_wrk_zips();
CALL bl_cl.sp_upsert_lkp_zips();

select* from bl_cl.mta_zips mz ;
select * from bl_cl.wrk_zips wz order by zip_src_id ;
select * from bl_cl.lkp_zips lz  order by zip_src_id ;

-- ======================================
-- Location Names
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_location_names()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_location_names;

    INSERT INTO bl_cl.mta_location_names (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('location_name_id',      'n.a.',                              'BIGINT',  'Generated via sequence',             FALSE, 'Surrogate key'),
        ('location_name_src_id',  'retailer_location_name',           'VARCHAR', 'COALESCE(field, ''n. a.'')',         FALSE, 'Location name from source'),
        ('zip_name',              'retailer_location_zip_code',       'VARCHAR', 'COALESCE(field, ''n. a.'')',         FALSE, 'Linked ZIP code'),
        ('location_name',         'same as location_name_src_id',     'VARCHAR', 'Same as location_name_src_id',      FALSE, 'Redundant location name'),
        ('source_system',         'n.a.',                             'VARCHAR', 'Static value',                       FALSE, 'Source system identifier'),
        ('source_entity',         'n.a.',                             'VARCHAR', 'Static value',                       FALSE, 'Source entity identifier'),
        ('insert_dt',             'n.a.',                             'DATE',    'CURRENT_DATE',                       FALSE, 'Insert timestamp'),
        ('update_dt',             'n.a.',                             'DATE',    'CURRENT_DATE',                       FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_location_names',
        v_rows,
        'Inserted metadata rows into mta_location_names.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_location_names',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_location_names()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_location_names;

    WITH all_location_zip AS (
        SELECT DISTINCT
            COALESCE(retailer_location_name::VARCHAR, 'n. a.') AS location_name_src_id,
            COALESCE(retailer_location_zip_code::VARCHAR, 'n. a.') AS zip_name,
            'sa_final_draw' AS source_system,
            'src_final_draw' AS source_entity
        FROM sa_final_draw.src_final_draw
        WHERE retailer_location_name IS NOT NULL

        UNION ALL

        SELECT DISTINCT
            COALESCE(retailer_location_name::VARCHAR, 'n. a.') AS location_name_src_id,
            COALESCE(retailer_location_zip_code::VARCHAR, 'n. a.') AS zip_name,
            'sa_final_scratch' AS source_system,
            'src_final_scratch' AS source_entity
        FROM sa_final_scratch.src_final_scratch
        WHERE retailer_location_name IS NOT NULL
    )

    INSERT INTO bl_cl.wrk_location_names (
        location_name_src_id,
        zip_name,
        source_system,
        source_entity,
        load_dt
    )
    SELECT
        location_name_src_id,
        zip_name,
        source_system,
        source_entity,
        CURRENT_DATE
    FROM all_location_zip;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_location_names',
        v_rows,
        'Loaded working location names successfully.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_location_names',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_location_names()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_location_candidates AS (
        SELECT DISTINCT
            location_name_src_id,
            zip_name,
            source_system,
            source_entity
        FROM bl_cl.wrk_location_names
    ),
    distinct_on_candidates AS (
        SELECT DISTINCT ON (location_name_src_id, source_system, source_entity)
            location_name_src_id,
            zip_name,
            source_system,
            source_entity
        FROM new_location_candidates
        ORDER BY location_name_src_id, source_system, source_entity
    ),
    surrogate_keys AS (
        SELECT
            location_name_src_id,
            nextval('bl_cl.ce_location_names_seq') AS location_name_id
        FROM (
            SELECT DISTINCT location_name_src_id
            FROM distinct_on_candidates
            WHERE location_name_src_id NOT IN (
                SELECT location_name_src_id FROM bl_cl.lkp_location_names
            )
        ) s
    ),
    zips_with_ids AS (
        SELECT
            COALESCE(sk.location_name_id, lkp.location_name_id) AS location_name_id,
            dc.location_name_src_id,
            dc.zip_name,
            dc.location_name_src_id AS location_name,
            dc.source_system,
            dc.source_entity
        FROM distinct_on_candidates dc
        LEFT JOIN surrogate_keys sk ON dc.location_name_src_id = sk.location_name_src_id
        LEFT JOIN bl_cl.lkp_location_names lkp ON dc.location_name_src_id = lkp.location_name_src_id
    )
    INSERT INTO bl_cl.lkp_location_names (
        location_name_id,
        location_name_src_id,
        zip_name,
        location_name,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT
        location_name_id,
        location_name_src_id,
        zip_name,
        location_name,
        source_system,
        source_entity,
        CURRENT_DATE,
        CURRENT_DATE
    FROM zips_with_ids
    ON CONFLICT (location_name_src_id, source_system, source_entity) DO UPDATE
    SET
        zip_name = EXCLUDED.zip_name,
        location_name = EXCLUDED.location_name,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_location_names',
        v_rows,
        'Upserted lookup location names successfully.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_location_names',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;


CALL bl_cl.sp_insert_meta_location_names();
CALL bl_cl.sp_load_wrk_location_names();
CALL bl_cl.sp_upsert_lkp_location_names();

select* from bl_cl.mta_location_names mln ;
select * from bl_cl.wrk_location_names wln order by location_name_src_id ;
select * from bl_cl.lkp_location_names lln order by location_name_src_id;

-- ======================================
-- Retailers
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_retailers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_retailers;

    INSERT INTO bl_cl.mta_retailers (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('retailer_license_number_id',     'n.a.',                          'BIGINT',   'Generated via sequence',     FALSE, 'Surrogate key'),
        ('retailer_license_number_src_id', 'retailer_license_number',       'VARCHAR',  'COALESCE(field, ''n. a.'')',  FALSE, 'Retailer license number'),
        ('location_name',                  'retailer_location_name',        'VARCHAR',  'COALESCE(field, ''n. a.'')',  TRUE,  'Retailer location name'),
        ('retailer_license_number_name',   'same as src_id',                'VARCHAR',  'Same as retailer_license_number_src_id', FALSE, 'Redundant field'),
        ('source_system',                  'n.a.',                          'VARCHAR',  'Static value',                FALSE, 'e.g. sa_final_draw'),
        ('source_entity',                  'n.a.',                          'VARCHAR',  'Static value',                FALSE, 'e.g. src_final_draw'),
        ('insert_dt',                      'n.a.',                          'DATE',     'CURRENT_DATE',                FALSE, 'Insert timestamp'),
        ('update_dt',                      'n.a.',                          'DATE',     'CURRENT_DATE',                FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_retailers',
        v_rows,
        'Inserted metadata rows into mta_retailers.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_insert_meta_retailers',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_retailers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_retailers;

    WITH all_retailers AS (
        SELECT DISTINCT
            COALESCE(retailer_license_number::VARCHAR, 'n. a.') AS retailer_license_number_src_id,
            COALESCE(retailer_location_name::VARCHAR, 'n. a.') AS location_name,
            'sa_final_draw' AS source_system,
            'src_final_draw' AS source_entity
        FROM sa_final_draw.src_final_draw
        WHERE retailer_license_number IS NOT NULL

        UNION ALL

        SELECT DISTINCT
            COALESCE(retailer_license_number::VARCHAR, 'n. a.') AS retailer_license_number_src_id,
            COALESCE(retailer_location_name::VARCHAR, 'n. a.') AS location_name,
            'sa_final_scratch' AS source_system,
            'src_final_scratch' AS source_entity
        FROM sa_final_scratch.src_final_scratch
        WHERE retailer_license_number IS NOT NULL
    ),
    deduped_retailers AS (
        SELECT DISTINCT ON (retailer_license_number_src_id, source_system, source_entity)        
            retailer_license_number_src_id,
            location_name,
            source_system,
            source_entity
        FROM all_retailers
    )
    INSERT INTO bl_cl.wrk_retailers (
        retailer_license_number_src_id,
        location_name,
        source_system,
        source_entity,
        load_dt
    )
    SELECT
        retailer_license_number_src_id,
        location_name,
        source_system,
        source_entity,
        CURRENT_DATE
    FROM deduped_retailers;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_retailers',
        v_rows,
        'Loaded working retailers successfully.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_load_wrk_retailers',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_retailers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_candidates AS (
        SELECT DISTINCT
            retailer_license_number_src_id,
            location_name,
            source_system,
            source_entity
        FROM bl_cl.wrk_retailers
    ),
    generated_ids AS (
        SELECT 
            retailer_license_number_src_id,
            NEXTVAL('bl_cl.t_mapping_retailer_license_seq') AS retailer_license_number_id
        FROM (
            SELECT DISTINCT retailer_license_number_src_id
            FROM new_candidates
        ) r
    ),
    retailers_with_ids AS (
        SELECT
            g.retailer_license_number_id,
            c.retailer_license_number_src_id,
            COALESCE(c.location_name, 'n. a.') AS location_name,
            c.source_system,
            c.source_entity
        FROM new_candidates c
        LEFT JOIN generated_ids g
          ON c.retailer_license_number_src_id = g.retailer_license_number_src_id
    ),
    aggregated_retailers AS (
        SELECT
            retailer_license_number_id,
            retailer_license_number_src_id,
            COALESCE(
                MAX(CASE WHEN location_name <> 'n. a.' THEN location_name END),
                'n. a.'
            ) AS location_name,
            source_system,
            source_entity
        FROM retailers_with_ids
        GROUP BY retailer_license_number_id, retailer_license_number_src_id, source_system, source_entity
    )
    INSERT INTO bl_cl.lkp_retailers (
        retailer_license_number_id,
        retailer_license_number_src_id,
        location_name,
        retailer_license_number_name,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT
        retailer_license_number_id,
        retailer_license_number_src_id,
        location_name,
        retailer_license_number_src_id,  -- redundant field
        source_system,
        source_entity,
        CURRENT_DATE,
        CURRENT_DATE
    FROM aggregated_retailers
    ON CONFLICT (retailer_license_number_src_id, source_system, source_entity) DO UPDATE
    SET
        location_name = EXCLUDED.location_name,
        retailer_license_number_name = EXCLUDED.retailer_license_number_name,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_retailers',
        v_rows,
        'Upserted lookup retailers successfully.',
        'INFO'
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl(
        'sp_upsert_lkp_retailers',
        0,
        'Error: ' || SQLERRM,
        'ERROR'
    );
    RAISE;
END;
$$;

CALL bl_cl.sp_insert_meta_retailers();
CALL bl_cl.sp_load_wrk_retailers();
CALL bl_cl.sp_upsert_lkp_retailers();

select* from bl_cl.mta_retailers mr ;
select * from bl_cl.wrk_retailers wr order by retailer_license_number_src_id ;
select * from bl_cl.lkp_retailers lr order by retailer_license_number_src_id ;
-- ======================================
-- Statuses
-- ======================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_statuses()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_statuses;

    INSERT INTO bl_cl.mta_statuses (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('status_id',     'n.a.',                  'BIGINT',  'Generated via sequence',    FALSE, 'Surrogate key'),
        ('status_src_id', 'employee_status',       'VARCHAR', 'COALESCE(field, ''n. a.'')', FALSE, 'Status source ID'),
        ('status_name',   'same as src_id',        'VARCHAR', 'Same as status_src_id',     FALSE, 'Status name (redundant)'),
        ('source_system', 'n.a.',                  'VARCHAR', 'Static value',              FALSE, 'Source system'),
        ('source_entity', 'n.a.',                  'VARCHAR', 'Static value',              FALSE, 'Source entity'),
        ('insert_dt',     'n.a.',                  'DATE',    'CURRENT_DATE',              FALSE, 'Insert timestamp'),
        ('update_dt',     'n.a.',                  'DATE',    'CURRENT_DATE',              FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl('sp_insert_meta_statuses', v_rows, 'Inserted metadata for statuses.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_insert_meta_statuses', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_statuses()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_statuses;

    WITH all_statuses AS (
        SELECT DISTINCT
            COALESCE(employee_status::VARCHAR, 'n. a.') AS status_src_id,
            'sa_final_scratch' AS source_system,
            'src_final_scratch' AS source_entity
        FROM sa_final_scratch.src_final_scratch
        WHERE employee_status IS NOT NULL
    ),
    deduped_statuses AS (
        SELECT DISTINCT ON (status_src_id, source_system, source_entity)
            status_src_id,
            source_system,
            source_entity
        FROM all_statuses
    )
    INSERT INTO bl_cl.wrk_statuses (
        status_src_id,
        source_system,
        source_entity,
        load_dt
    )
    SELECT
        status_src_id,
        source_system,
        source_entity,
        CURRENT_DATE
    FROM deduped_statuses;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl('sp_load_wrk_statuses', v_rows, 'Loaded working statuses.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_load_wrk_statuses', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_statuses()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_candidates AS (
        SELECT DISTINCT status_src_id, source_system, source_entity
        FROM bl_cl.wrk_statuses
    ),
    existing_statuses AS (
        SELECT status_src_id, source_system, source_entity, status_id
        FROM bl_cl.lkp_statuses
    ),
    statuses_to_insert AS (
        SELECT nc.status_src_id, nc.source_system, nc.source_entity
        FROM new_candidates nc
        LEFT JOIN existing_statuses es
          ON nc.status_src_id = es.status_src_id
         AND nc.source_system = es.source_system
         AND nc.source_entity = es.source_entity
        WHERE es.status_id IS NULL
    ),
    generated_ids AS (
        SELECT
            status_src_id,
            source_system,
            source_entity,
            NEXTVAL('bl_cl.t_mapping_status_seq') AS status_id
        FROM statuses_to_insert
    ),
    upsert_data AS (
        SELECT
            g.status_id,
            g.status_src_id,
            g.source_system,
            g.source_entity,
            g.status_src_id AS status_name,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE AS update_dt
        FROM generated_ids g

        UNION ALL

        SELECT
            es.status_id,
            es.status_src_id,
            es.source_system,
            es.source_entity,
            es.status_src_id AS status_name,
            l.insert_dt,
            CURRENT_DATE AS update_dt
        FROM existing_statuses es
        JOIN bl_cl.lkp_statuses l ON es.status_id = l.status_id
    )
    INSERT INTO bl_cl.lkp_statuses (
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
    FROM upsert_data
    ON CONFLICT (status_src_id, source_system, source_entity) DO UPDATE
    SET
        status_name = EXCLUDED.status_name,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_3nf.p_log_etl('sp_upsert_lkp_statuses', v_rows, 'Upserted statuses successfully.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_upsert_lkp_statuses', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CALL bl_cl.sp_insert_meta_statuses();
CALL bl_cl.sp_load_wrk_statuses();
CALL bl_cl.sp_upsert_lkp_statuses();
select* from bl_cl.mta_statuses ms ;
select * from bl_cl.wrk_statuses ws order by ws.status_src_id ;
select * from bl_cl.lkp_statuses ls order by ls.status_src_id;
-- ======================================
-- Departments
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_departments()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_departments;

    INSERT INTO bl_cl.mta_departments (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('department_id',       'n.a.',                     'BIGINT',  'Generated via sequence',              FALSE, 'Surrogate key'),
        ('department_src_id',   'employee_department',      'VARCHAR', 'COALESCE(field, ''n. a.'')',          FALSE, 'Department source identifier'),
        ('department_name',     'same as src_id',           'VARCHAR', 'Same as department_src_id',           FALSE, 'Redundant department name'),
        ('source_system',       'n.a.',                     'VARCHAR', 'Static value, e.g. sa_final_scratch', FALSE, 'Source system name'),
        ('source_entity',       'n.a.',                     'VARCHAR', 'Static value, e.g. src_final_scratch',FALSE, 'Source entity name'),
        ('insert_dt',           'n.a.',                     'DATE',    'CURRENT_DATE',                        FALSE, 'Insert timestamp'),
        ('update_dt',           'n.a.',                     'DATE',    'CURRENT_DATE',                        FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_insert_meta_departments', v_rows, 'Inserted department metadata.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_insert_meta_departments', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_departments()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_departments;

    WITH all_departments AS (
        SELECT DISTINCT
            COALESCE(employee_department::VARCHAR, 'n. a.') AS department_src_id,
            'sa_final_scratch' AS source_system,
            'src_final_scratch' AS source_entity
        FROM sa_final_scratch.src_final_scratch
        WHERE employee_department IS NOT NULL
    ),
    deduped_departments AS (
        SELECT DISTINCT ON (department_src_id, source_system, source_entity)
            department_src_id,
            source_system,
            source_entity
        FROM all_departments
    )
    INSERT INTO bl_cl.wrk_departments (
        department_src_id,
        source_system,
        source_entity,
        load_dt
    )
    SELECT
        department_src_id,
        source_system,
        source_entity,
        CURRENT_DATE
    FROM deduped_departments;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_load_wrk_departments', v_rows, 'Loaded working departments.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_load_wrk_departments', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_departments()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_candidates AS (
        SELECT DISTINCT department_src_id, source_system, source_entity
        FROM bl_cl.wrk_departments
    ),
    existing_departments AS (
        SELECT department_src_id, source_system, source_entity, department_id
        FROM bl_cl.lkp_departments
    ),
    departments_to_insert AS (
        SELECT nc.department_src_id, nc.source_system, nc.source_entity
        FROM new_candidates nc
        LEFT JOIN existing_departments ed
          ON nc.department_src_id = ed.department_src_id
         AND nc.source_system = ed.source_system
         AND nc.source_entity = ed.source_entity
        WHERE ed.department_id IS NULL
    ),
    generated_ids AS (
        SELECT
            department_src_id,
            source_system,
            source_entity,
            NEXTVAL('bl_cl.t_mapping_department_seq') AS department_id
        FROM departments_to_insert
    ),
    upsert_data AS (
        SELECT
            g.department_id,
            g.department_src_id,
            g.source_system,
            g.source_entity,
            g.department_src_id AS department_name,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE AS update_dt
        FROM generated_ids g

        UNION ALL

        SELECT
            ed.department_id,
            ed.department_src_id,
            ed.source_system,
            ed.source_entity,
            ed.department_src_id AS department_name,
            l.insert_dt,
            CURRENT_DATE AS update_dt
        FROM existing_departments ed
        JOIN bl_cl.lkp_departments l ON ed.department_id = l.department_id
    )
    INSERT INTO bl_cl.lkp_departments (
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
    FROM upsert_data
    ON CONFLICT (department_src_id, source_system, source_entity) DO UPDATE
    SET
        department_name = EXCLUDED.department_name,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_upsert_lkp_departments', v_rows, 'Upserted departments successfully.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_upsert_lkp_departments', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CALL bl_cl.sp_insert_meta_departments();
CALL bl_cl.sp_load_wrk_departments();
CALL bl_cl.sp_upsert_lkp_departments();
select* from bl_cl.mta_departments md ;
select * from bl_cl.wrk_departments wd order by department_src_id ;
select * from bl_cl.lkp_departments ld order by department_src_id;

-- ======================================
-- Employees
-- ======================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_employees()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_employees;

    INSERT INTO bl_cl.mta_employees (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    )
    VALUES
        ('employee_id',               'n.a.',                         'BIGINT',   'Generated via sequence',                       FALSE, 'Surrogate key'),
        ('employee_src_id',           'employee_id',                  'VARCHAR',  'COALESCE(field, ''n. a.'')',                   FALSE, 'Employee source ID'),
        ('employee_name',             'employee_name',                'VARCHAR',  'COALESCE(field, ''n. a.'')',                   FALSE, 'Employee full name'),
        ('employee_department_name',  'employee_department',          'VARCHAR',  'COALESCE(field, ''n. a.'')',                   FALSE, 'Employee department name'),
        ('employee_status_name',      'employee_status',              'VARCHAR',  'COALESCE(field, ''n. a.'')',                   FALSE, 'Employee status name'),
        ('employee_email',            'employee_email',               'VARCHAR',  'COALESCE(field, ''n. a.'')',                   FALSE, 'Employee email'),
        ('employee_phone',            'employee_phone',               'VARCHAR',  'COALESCE(field, ''n. a.'')',                   FALSE, 'Employee phone number'),
        ('employee_hire_dt',          'employee_hire_dt',             'DATE',     'COALESCE(field, DATE ''1900-01-01'')',         FALSE, 'Employee hire date'),
        ('employee_salary',           'employee_salary',              'FLOAT',    'COALESCE(field, -1)',                           FALSE, 'Employee salary'),
        ('source_system',             'n.a.',                         'VARCHAR',  'Static value',                                 FALSE, 'Source system name'),
        ('source_entity',             'n.a.',                         'VARCHAR',  'Static value',                                 FALSE, 'Source entity name'),
        ('insert_dt',                 'n.a.',                         'DATE',     'CURRENT_DATE',                                 FALSE, 'Insert timestamp'),
        ('update_dt',                 'n.a.',                         'DATE',     'CURRENT_DATE',                                 FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_insert_meta_employees', v_rows, 'Inserted employee metadata.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_insert_meta_employees', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_employees()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_employees;

    WITH combined_employees AS (
        SELECT
            employee_id::VARCHAR(255) AS employee_src_id,
            employee_name,
            employee_department AS employee_department_name,
            employee_status AS employee_status_name,
            'n. a.' as employee_email,
            'n. a.' as employee_phone,
            DATE '1900-01-01' as employee_hire_dt,
            -1 as employee_salary,
            'sa_final_scratch' AS source_system,
            'src_final_scratch' AS source_entity
        FROM sa_final_scratch.src_final_scratch

        UNION ALL

        SELECT
            employee_id::VARCHAR(255) AS employee_src_id,
            'n. a.' AS employee_name,
            'n. a.' AS employee_department_name,
            'n. a.' AS employee_status_name,
            employee_email,
            employee_phone,
            employee_hire_dt::DATE,
            employee_salary::FLOAT,
            'sa_final_draw' AS source_system,
            'src_final_draw' AS source_entity
        FROM sa_final_draw.src_final_draw
    ),
    deduped_employees AS (
        SELECT DISTINCT ON (employee_src_id, source_system, source_entity)
            employee_src_id,
            employee_name,
            employee_department_name,
            employee_status_name,
            employee_email,
            employee_phone,
            employee_hire_dt,
            employee_salary,
            source_system,
            source_entity
        FROM combined_employees
        ORDER BY employee_src_id, source_system, source_entity, employee_hire_dt DESC
    )
    INSERT INTO bl_cl.wrk_employees (
        employee_src_id,
        employee_name,
        employee_department_name,
        employee_status_name,
        employee_email,
        employee_phone,
        employee_hire_dt,
        employee_salary,
        source_system,
        source_entity,
        load_dt
    )
    SELECT
        employee_src_id,
        employee_name,
        employee_department_name,
        employee_status_name,
        employee_email,
        employee_phone,
        employee_hire_dt,
        employee_salary,
        source_system,
        source_entity,
        CURRENT_DATE
    FROM deduped_employees;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_load_wrk_employees', v_rows, 'Loaded working employees.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_load_wrk_employees', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_employees()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH new_candidates AS (
        SELECT DISTINCT
            employee_src_id,
            employee_name,
            employee_department_name,
            employee_status_name,
            employee_email,
            employee_phone,
            employee_hire_dt,
            employee_salary,
            source_system,
            source_entity
        FROM bl_cl.wrk_employees
    ),
    generated_ids AS (
        SELECT DISTINCT ON (employee_src_id)
            employee_src_id,
            NEXTVAL('bl_cl.t_mapping_employees_seq') AS employee_id
        FROM new_candidates
    ),
    upsert_data AS (
        SELECT
            g.employee_id,
            nc.employee_src_id,
            nc.employee_name,
            nc.employee_department_name,
            nc.employee_status_name,
            nc.employee_email,
            nc.employee_phone,
            nc.employee_hire_dt,
            nc.employee_salary,
            nc.source_system,
            nc.source_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE AS update_dt
        FROM new_candidates nc
        LEFT JOIN generated_ids g ON g.employee_src_id = nc.employee_src_id
    )
    INSERT INTO bl_cl.lkp_employees (
        employee_id,
        employee_src_id,
        employee_name,
        employee_department_name,
        employee_status_name,
        employee_email,
        employee_phone,
        employee_hire_dt,
        employee_salary,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT
        employee_id,
        employee_src_id,
        employee_name,
        employee_department_name,
        employee_status_name,
        employee_email,
        employee_phone,
        employee_hire_dt,
        employee_salary,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    FROM upsert_data
    ON CONFLICT (employee_src_id, source_system, source_entity) DO UPDATE
    SET
        employee_name = EXCLUDED.employee_name,
        employee_department_name = EXCLUDED.employee_department_name,
        employee_status_name = EXCLUDED.employee_status_name,
        employee_email = EXCLUDED.employee_email,
        employee_phone = EXCLUDED.employee_phone,
        employee_hire_dt = EXCLUDED.employee_hire_dt,
        employee_salary = EXCLUDED.employee_salary,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_upsert_lkp_employees', v_rows, 'Upserted employees successfully.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_upsert_lkp_employees', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CALL bl_cl.sp_insert_meta_employees();
CALL bl_cl.sp_load_wrk_employees();
CALL bl_cl.sp_upsert_lkp_employees();

select* from bl_cl.mta_employees me ;
select * from bl_cl.wrk_employees we order by we.employee_src_id ;
select * from bl_cl.lkp_employees le order by le.employee_src_id ;

-- ======================================
-- Customers
-- ======================================
CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_customers;

    INSERT INTO bl_cl.mta_customers (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    ) VALUES
        ('customer_id',              'n.a.',                      'BIGINT',   'Generated using sequence bl_cl.t_mapping_customers_seq', FALSE, 'Surrogate primary key'),
        ('customer_src_id',          'customer_id',             'VARCHAR',  'Cast from source system ID, used for tracking',          FALSE, 'Natural business key'),
        ('zip_name',                 'customer_zip_code',       'VARCHAR',  'COALESCE(zip, ''n.a.'')',                                TRUE,  'Default for missing ZIP'),
        ('customer_registration_dt', 'customer_registration_dt','DATE',     'COALESCE(date, ''1900-01-01'')',                          FALSE, 'Default if not provided'),
        ('customer_name',            'customer_name',           'VARCHAR',  'COALESCE(name, ''n.a.'')',                                TRUE,  'Standard null handling'),
        ('customer_gender',          'customer_gender',         'VARCHAR',  'COALESCE(gender, ''n.a.'')',                              TRUE,  'n.a.'),
        ('customer_dob',             'customer_dob',            'DATE',     'COALESCE(dob, ''1900-01-01'')',                            FALSE, 'n.a.'),
        ('customer_email',           'customer_email',          'VARCHAR',  'COALESCE(email, ''n.a.'')',                               TRUE,  'n.a.'),
        ('customer_phone',           'customer_phone',          'VARCHAR',  'COALESCE(phone, ''n.a.'')',                               TRUE,  'n.a.'),
        ('source_system',            'source_system',           'VARCHAR',  'Direct mapping',                                          FALSE, 'n.a.'),
        ('source_entity',            'source_entity',           'VARCHAR',  'Direct mapping',                                          FALSE, 'n.a.'),
        ('is_active',                'n.a.',                      'BOOLEAN',  'Always TRUE on insert, set to FALSE on update',           FALSE, 'SCD2 flag'),
        ('insert_dt',                'n.a.',                      'DATE',     'CURRENT_DATE on insert',                                  FALSE, 'Tracking load date'),
        ('start_dt',                 'n.a.',                      'DATE',     'CURRENT_DATE on insert',                                  FALSE, 'SCD2 start date'),
        ('end_dt',                   'n.a.',                      'DATE',     '''9999-12-31'' on insert',                                 FALSE, 'SCD2 end date default');

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_insert_meta_customers', v_rows, 'Inserted customer metadata.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_insert_meta_customers', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;
CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_customers;

    WITH all_customers_raw AS (
        SELECT
            customer_id::VARCHAR                     AS customer_src_id,
            customer_name,
            customer_gender,
            customer_dob::DATE,
            NULL::VARCHAR                            AS customer_zip_code,
            NULL::DATE                               AS customer_registration_dt,
            NULL::VARCHAR                            AS customer_email,
            NULL::VARCHAR                            AS customer_phone,
            'sa_final_scratch'                       AS source_system,
            'src_final_scratch'                      AS source_entity
        FROM sa_final_scratch.src_final_scratch
        WHERE customer_id IS NOT NULL

        UNION ALL

        SELECT
            customer_id::VARCHAR                     AS customer_src_id,
            NULL                                     AS customer_name,
            NULL                                     AS customer_gender,
            NULL                                     AS customer_dob,
            customer_zip_code,
            customer_registration_dt::DATE,
            customer_email,
            customer_phone,
            'sa_final_draw'                          AS source_system,
            'src_final_draw'                         AS source_entity
        FROM sa_final_draw.src_final_draw
        WHERE customer_id IS NOT NULL
    ),
    all_customers AS (
        SELECT DISTINCT * FROM all_customers_raw
    ),
    cleaned_customers AS (
        SELECT
            customer_src_id,
            COALESCE(TRIM(customer_zip_code), 'n.a.')             AS zip_name,
            COALESCE(customer_registration_dt, DATE '1900-01-01') AS customer_registration_dt,
            COALESCE(TRIM(customer_name), 'n.a.')                 AS customer_name,
            COALESCE(TRIM(customer_gender), 'n.a.')               AS customer_gender,
            COALESCE(customer_dob, DATE '1900-01-01')             AS customer_dob,
            COALESCE(TRIM(customer_email), 'n.a.')                AS customer_email,
            COALESCE(TRIM(customer_phone), 'n.a.')                AS customer_phone,
            source_system,
            source_entity,
            CURRENT_DATE                                          AS load_dt
        FROM all_customers
    )
    INSERT INTO bl_cl.wrk_customers (
        customer_src_id,
        zip_name,
        customer_registration_dt,
        customer_name,
        customer_gender,
        customer_dob,
        customer_email,
        customer_phone,
        source_system,
        source_entity,
        load_dt
    )
    SELECT * FROM cleaned_customers;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_load_wrk_customers', v_rows, 'Loaded working customers.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_load_wrk_customers', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;
CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    surr_id BIGINT;
    v_rows INTEGER;
BEGIN
    CREATE TEMP TABLE tmp_customers_to_upsert AS
    WITH distinct_customers AS (
        SELECT DISTINCT ON (customer_src_id, source_system, source_entity)
            customer_src_id,
            zip_name,
            customer_registration_dt,
            customer_name,
            customer_gender,
            customer_dob,
            customer_email,
            customer_phone,
            source_system,
            source_entity
        FROM bl_cl.wrk_customers
    ),
    unique_customers AS (
        SELECT DISTINCT customer_src_id
        FROM distinct_customers
    ),
    generated_ids AS (
        SELECT
            customer_src_id,
            NEXTVAL('bl_cl.t_mapping_customers_seq') AS customer_id
        FROM unique_customers
    )
    SELECT 
        ci.customer_id,
        dc.customer_src_id,
        dc.zip_name,
        dc.customer_registration_dt,
        dc.customer_name,
        dc.customer_gender,
        dc.customer_dob,
        dc.customer_email,
        dc.customer_phone,
        dc.source_system,
        dc.source_entity
    FROM distinct_customers dc
    JOIN generated_ids ci
      ON dc.customer_src_id = ci.customer_src_id;

    FOR rec IN SELECT * FROM tmp_customers_to_upsert LOOP

        IF EXISTS (
            SELECT 1 FROM bl_cl.lkp_customers
            WHERE customer_src_id = rec.customer_src_id
              AND source_system = rec.source_system
              AND source_entity = rec.source_entity
              AND is_active = TRUE
        ) THEN

            IF EXISTS (
                SELECT 1 FROM bl_cl.lkp_customers t
                WHERE t.customer_src_id = rec.customer_src_id
                  AND t.source_system = rec.source_system
                  AND t.source_entity = rec.source_entity
                  AND t.is_active = TRUE
                  AND t.customer_name IS NOT DISTINCT FROM rec.customer_name
                  AND t.customer_gender IS NOT DISTINCT FROM rec.customer_gender
                  AND t.customer_dob IS NOT DISTINCT FROM rec.customer_dob
                  AND t.customer_email IS NOT DISTINCT FROM rec.customer_email
                  AND t.customer_phone IS NOT DISTINCT FROM rec.customer_phone
                  AND t.zip_name IS NOT DISTINCT FROM rec.zip_name
                  AND t.customer_registration_dt IS NOT DISTINCT FROM rec.customer_registration_dt
            ) THEN
                CONTINUE;
            ELSE
                SELECT customer_id INTO surr_id
                FROM bl_cl.lkp_customers
                WHERE customer_src_id = rec.customer_src_id
                  AND source_system = rec.source_system
                  AND source_entity = rec.source_entity
                  AND is_active = TRUE
                LIMIT 1;

                UPDATE bl_cl.lkp_customers
                SET is_active = FALSE,
                    end_dt = CURRENT_DATE
                WHERE customer_src_id = rec.customer_src_id
                  AND source_system = rec.source_system
                  AND source_entity = rec.source_entity
                  AND is_active = TRUE;

                INSERT INTO bl_cl.lkp_customers (
                    customer_id,
                    customer_src_id,
                    zip_name,
                    customer_registration_dt,
                    customer_name,
                    customer_gender,
                    customer_dob,
                    customer_email,
                    customer_phone,
                    source_system,
                    source_entity,
                    is_active,
                    insert_dt,
                    start_dt,
                    end_dt
                ) VALUES (
                    surr_id,
                    rec.customer_src_id,
                    COALESCE(rec.zip_name, 'n.a.'),
                    COALESCE(rec.customer_registration_dt, DATE '1900-01-01'),
                    COALESCE(rec.customer_name, 'n.a.'),
                    COALESCE(rec.customer_gender, 'n.a.'),
                    COALESCE(rec.customer_dob, DATE '1900-01-01'),
                    COALESCE(rec.customer_email, 'n.a.'),
                    COALESCE(rec.customer_phone, 'n.a.'),
                    rec.source_system,
                    rec.source_entity,
                    TRUE,
                    CURRENT_DATE,
                    CURRENT_DATE,
                    DATE '9999-12-31'
                );
            END IF;
        ELSE
            INSERT INTO bl_cl.lkp_customers (
                customer_id,
                customer_src_id,
                zip_name,
                customer_registration_dt,
                customer_name,
                customer_gender,
                customer_dob,
                customer_email,
                customer_phone,
                source_system,
                source_entity,
                is_active,
                insert_dt,
                start_dt,
                end_dt
            ) VALUES (
                rec.customer_id,
                rec.customer_src_id,
                COALESCE(rec.zip_name, 'n.a.'),
                COALESCE(rec.customer_registration_dt, DATE '1900-01-01'),
                COALESCE(rec.customer_name, 'n.a.'),
                COALESCE(rec.customer_gender, 'n.a.'),
                COALESCE(rec.customer_dob, DATE '1900-01-01'),
                COALESCE(rec.customer_email, 'n.a.'),
                COALESCE(rec.customer_phone, 'n.a.'),
                rec.source_system,
                rec.source_entity,
                TRUE,
                CURRENT_DATE,
                CURRENT_DATE,
                DATE '9999-12-31'
            );
        END IF;
    END LOOP;

    DROP TABLE IF EXISTS tmp_customers_to_upsert;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_upsert_lkp_customers', v_rows, 'Upserted customers with SCD2 handling.', 'INFO');

EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_upsert_lkp_customers', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CALL bl_cl.sp_insert_meta_customers();
CALL bl_cl.sp_load_wrk_customers();
CALL bl_cl.sp_upsert_lkp_customers();

select* from bl_cl.mta_customers mc ;
select * from bl_cl.wrk_customers wc order by wc.customer_src_id ;
select * from bl_cl.lkp_customers lc order by lc.customer_src_id ;


-- ============================================
-- Sales
-- ============================================

CREATE OR REPLACE PROCEDURE bl_cl.sp_insert_meta_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    DELETE FROM bl_cl.mta_sales;

    INSERT INTO bl_cl.mta_sales (
        column_name, source_column_name, data_type, transformation_rule, is_nullable, notes
    ) VALUES
        ('game_number_src_id',             'game_number',              'VARCHAR', 'CAST(game_number AS VARCHAR)',               FALSE, 'Game number identifier'),
        ('customer_src_id',                'customer_id',              'VARCHAR', 'CAST(customer_id AS VARCHAR)',               FALSE, 'Customer identifier'),
        ('employee_src_id',                'employee_id',              'VARCHAR', 'CAST(employee_id AS VARCHAR)',               FALSE, 'Employee identifier'),
        ('retailer_license_number_src_id', 'retailer_license_number',  'VARCHAR', 'CAST(retailer_license_number AS VARCHAR)',   FALSE, 'Retailer license'),
        ('payment_method_src_id',          'payment_method_id',        'VARCHAR', 'CAST(payment_method_id AS VARCHAR)',         FALSE, 'Payment method'),
        ('event_dt',                       'transaction_dt',           'DATE',    'CAST(transaction_dt AS DATE)',               FALSE, 'Event date'),
        ('tickets_bought',                 'tickets_bought',           'INT',     'Default -1 if null',                         TRUE,  'Ticket quantity'),
        ('payout',                         'payout',                   'FLOAT',   'Default -1 if null',                         TRUE,  'Payout amount'),
        ('sales',                          'sales',                    'FLOAT',   'Default -1 if null',                         TRUE,  'Sales value'),
        ('ticket_price',                   'ticket_price',             'FLOAT',   'Default -1 if null',                         TRUE,  'Price per ticket'),
        ('source_system',                  'N/A',                      'VARCHAR', 'Static: source system',                      FALSE, 'E.g., sa_final_scratch'),
        ('source_entity',                  'N/A',                      'VARCHAR', 'Static: source entity',                      FALSE, 'E.g., src_final_scratch'),
        ('insert_dt',                      'N/A',                      'DATE',    'CURRENT_DATE',                                FALSE, 'Insert timestamp'),
        ('update_dt',                      'N/A',                      'DATE',    'CURRENT_DATE',                                FALSE, 'Update timestamp');

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_insert_meta_sales', v_rows, 'Inserted sales metadata.', 'INFO');
EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_insert_meta_sales', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.sp_load_wrk_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE TABLE bl_cl.wrk_sales;

    INSERT INTO bl_cl.wrk_sales (
        game_number_src_id,
        customer_src_id,
        employee_src_id,
        retailer_license_number_src_id,
        payment_method_src_id,
        transaction_dt,
        tickets_bought,
        payout,
        sales,
        ticket_price,
        source_system,
        source_entity
    )
    SELECT DISTINCT
        CAST(game_number AS VARCHAR(100)),
        CAST(customer_id AS VARCHAR(100)),
        CAST(employee_id AS VARCHAR(100)),
        CAST(retailer_license_number AS VARCHAR(100)),
        CAST(payment_method_id AS VARCHAR(100)),
        transaction_dt::TIMESTAMP,
        tickets_bought::INT,
        payout::NUMERIC,
        sales::NUMERIC,
        ticket_price::NUMERIC,
        'sa_final_scratch',
        'src_final_scratch'
    FROM sa_final_scratch.src_final_scratch
    WHERE customer_id IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        CAST(game_number AS VARCHAR(100)),
        CAST(customer_id AS VARCHAR(100)),
        CAST(employee_id AS VARCHAR(100)),
        CAST(retailer_license_number AS VARCHAR(100)),
        CAST(payment_method_id AS VARCHAR(100)),
        transaction_dt::TIMESTAMP,
        tickets_bought::INT,
        payout::NUMERIC,
        sales::NUMERIC,
        ticket_price::NUMERIC,
        'sa_final_draw',
        'src_final_draw'
    FROM sa_final_draw.src_final_draw
    WHERE customer_id IS NOT NULL;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_load_wrk_sales', v_rows, 'Loaded working sales data.', 'INFO');
EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_load_wrk_sales', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.sp_upsert_lkp_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    WITH deduplicated_sales AS (
        SELECT DISTINCT ON (
            game_number_src_id,
            customer_src_id,
            employee_src_id,
            retailer_license_number_src_id,
            payment_method_src_id,
            transaction_dt,
            source_system,
            source_entity
        )
            game_number_src_id,
            customer_src_id,
            employee_src_id,
            retailer_license_number_src_id,
            payment_method_src_id,
            transaction_dt,
            tickets_bought,
            payout,
            sales,
            ticket_price,
            source_system,
            source_entity
        FROM bl_cl.wrk_sales
        ORDER BY 
            game_number_src_id,
            customer_src_id,
            employee_src_id,
            retailer_license_number_src_id,
            payment_method_src_id,
            source_system,
            source_entity,
            transaction_dt DESC
    )
    INSERT INTO bl_cl.lkp_sales (
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
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT
        game_number_src_id,
        customer_src_id,
        employee_src_id,
        retailer_license_number_src_id,
        payment_method_src_id,
        COALESCE(transaction_dt::DATE, DATE '1900-01-01'),
        COALESCE(tickets_bought, -1),
        COALESCE(payout, -1),
        COALESCE(sales, -1),
        COALESCE(ticket_price, -1),
        source_system,
        source_entity,
        CURRENT_DATE,
        CURRENT_DATE
    FROM deduplicated_sales
    ON CONFLICT (
        game_number_src_id,
        customer_src_id,
        employee_src_id,
        retailer_license_number_src_id,
        payment_method_src_id,
        event_dt,
        source_system,
        source_entity
    )
    DO UPDATE SET
        tickets_bought = EXCLUDED.tickets_bought,
        payout = EXCLUDED.payout,
        sales = EXCLUDED.sales,
        ticket_price = EXCLUDED.ticket_price,
        update_dt = CURRENT_DATE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL bl_3nf.p_log_etl('sp_upsert_lkp_sales', v_rows, 'Upserted sales records.', 'INFO');
EXCEPTION WHEN OTHERS THEN
    CALL bl_3nf.p_log_etl('sp_upsert_lkp_sales', 0, 'Error: ' || SQLERRM, 'ERROR');
    RAISE;
END;
$$;

CALL bl_cl.sp_insert_meta_sales();
CALL bl_cl.sp_load_wrk_sales();
CALL bl_cl.sp_upsert_lkp_sales();

select* from bl_cl.mta_sales ms ;
select * from bl_cl.wrk_sales ws order by ws.customer_src_id ;
select * from bl_cl.lkp_sales ls order by ls.customer_src_id ;

select count(*) from bl_cl.lkp_sales ls ;