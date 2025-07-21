-- ======================================
-- Game Types
-- ======================================
GRANT EXECUTE ON FUNCTION bl_3nf.p_load_ce_game_types() TO bl_cl;
GRANT INSERT,  select, UPDATE ON bl_3nf.ce_game_types TO bl_cl;

-- ======================================
-- Game Categories
-- ======================================
GRANT EXECUTE ON FUNCTION bl_3nf.p_load_ce_game_categories() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.ce_game_categories TO bl_cl;

-- ======================================
-- Game Numbers
-- ======================================
GRANT EXECUTE ON FUNCTION bl_3nf.p_load_ce_game_numbers() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.ce_game_numbers TO bl_cl;

-- ======================================
-- Payment Methods
-- ======================================
GRANT EXECUTE ON FUNCTION bl_3nf.p_load_ce_payment_methods() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.ce_payment_methods TO bl_cl;


-- ======================================
-- States
-- ======================================
GRANT EXECUTE ON FUNCTION bl_3nf.p_load_ce_states() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.ce_states TO bl_cl;



-- ======================================
-- Cities
-- ======================================
GRANT EXECUTE ON FUNCTION bl_3nf.p_load_ce_cities() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.ce_cities TO bl_cl;



-- ======================================
-- Zip Codes
-- ======================================

GRANT EXECUTE ON FUNCTION bl_3nf.p_load_ce_zip() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.ce_zip TO bl_cl;

-- ======================================
-- Location Names
-- ======================================
GRANT EXECUTE ON FUNCTION bl_3nf.p_load_ce_location_names() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.ce_location_names TO bl_cl;


-- ======================================
-- Retailer License Numbers
-- ======================================
GRANT EXECUTE ON PROCEDURE bl_3nf.load_retailer_license_numbers() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.CE_RETAILER_LICENSE_NUMBERS TO bl_cl;
-- ======================================
-- Statuses
-- ======================================

GRANT EXECUTE ON PROCEDURE bl_3nf.p_load_ce_statuses() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.CE_STATUSES TO bl_cl;


-- ======================================
-- Departments
-- ======================================
GRANT EXECUTE ON PROCEDURE bl_3nf.p_load_ce_departments() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.CE_DEPARTMENTS TO bl_cl;

-- ======================================
-- Employees
-- ======================================
GRANT EXECUTE ON PROCEDURE bl_3nf.p_load_ce_employees() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.CE_EMPLOYEES TO bl_cl;

-- ======================================
-- Customers SCD
-- ======================================
GRANT EXECUTE ON PROCEDURE bl_3nf.p_load_ce_customers_scd() TO bl_cl;
GRANT INSERT, SELECT, UPDATE ON bl_3nf.CE_CUSTOMERS_SCD TO bl_cl;

-- ======================================
-- Sales
-- ======================================

GRANT EXECUTE ON PROCEDURE bl_3nf.p_load_ce_sales() TO bl_cl;
GRANT INSERT, SELECT ON bl_3nf.CE_SALES TO bl_cl;



CREATE TABLE IF NOT EXISTS bl_3nf.etl_logs (
    log_id BIGSERIAL PRIMARY KEY,
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    procedure_name TEXT NOT NULL,
    rows_affected INT,
    log_message TEXT,
    log_level TEXT CHECK (log_level IN ('INFO', 'ERROR', 'WARN')) DEFAULT 'INFO'
);

CREATE OR REPLACE PROCEDURE bl_3nf.p_log_etl(
    p_procedure_name TEXT,
    p_rows_affected INT,
    p_log_message TEXT,
    p_log_level TEXT DEFAULT 'INFO'
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO bl_3nf.etl_logs (
        procedure_name,
        rows_affected,
        log_message,
        log_level
    )
    VALUES (
        p_procedure_name,
        p_rows_affected,
        p_log_message,
        UPPER(p_log_level)
    );
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Failed to log ETL event: %', SQLERRM;
END;
$$;

