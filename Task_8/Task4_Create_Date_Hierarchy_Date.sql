-- Drop schema and everything in it (optional, only if you want a full reset)
-- DROP SCHEMA IF EXISTS bl_dm CASCADE;

-- OR just drop the table
DROP TABLE IF EXISTS bl_dm.dim_date;

-- Recreate schema (safe even if it exists)
CREATE SCHEMA IF NOT EXISTS bl_dm;



-- Make sure the table exists before running this
CREATE TABLE IF NOT EXISTS bl_dm.dim_date (
    event_dt DATE primary KEY,
    fiscal_year INT,
    fiscal_month INT,
    fiscal_month_name_number VARCHAR(50),
    calendar_year INT,
    calendar_month INT,
    calendar_month_name_number VARCHAR(50),
    month_ending_date DATE
);

-- Now insert using DO block
DO $$
DECLARE
    dt DATE := '2010-01-01';
    end_dt DATE := '2022-12-31';
    fiscal_year INT;
    fiscal_month INT;
    fiscal_month_name VARCHAR(50);
    calendar_year INT;
    calendar_month INT;
    calendar_month_name VARCHAR(50);
    month_ending DATE;
BEGIN
    WHILE dt <= end_dt LOOP
        fiscal_year := EXTRACT(YEAR FROM dt)::INT;
        fiscal_month := EXTRACT(MONTH FROM dt)::INT;
        fiscal_month_name := '04-' || TRIM(TO_CHAR(dt, 'Month'));
        calendar_year := fiscal_year;
        calendar_month := fiscal_month;
        calendar_month_name := '01-' || TRIM(TO_CHAR(dt, 'Month'));
        month_ending := (DATE_TRUNC('month', dt) + INTERVAL '1 month - 1 day')::DATE;

        INSERT INTO bl_dm.dim_date (
            event_dt,
            fiscal_year,
            fiscal_month,
            fiscal_month_name_number,
            calendar_year,
            calendar_month,
            calendar_month_name_number,
            month_ending_date
        )
        VALUES (
            dt,
            fiscal_year,
            fiscal_month,
            fiscal_month_name,
            calendar_year,
            calendar_month,
            calendar_month_name,
            month_ending
        );

        dt := dt + INTERVAL '1 day';
    END LOOP;

    COMMIT;
END;
$$;

SELECT * FROM bl_dm.dim_date;