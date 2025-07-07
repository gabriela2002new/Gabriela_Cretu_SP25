CREATE SCHEMA IF NOT EXISTS bl_dm;

-- Make sure the table exists before running this
CREATE TABLE IF NOT EXISTS bl_dm.dim_date (
    date_id BIGINT PRIMARY KEY,
    event_dt DATE,
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
    dt DATE := '2021-01-01';
    end_dt DATE := '2022-12-31';
    date_id BIGINT;
    fiscal_year INT;
    fiscal_month INT;
    fiscal_month_name VARCHAR(50);
    calendar_year INT;
    calendar_month INT;
    calendar_month_name VARCHAR(50);
    month_ending DATE;
BEGIN
    WHILE dt <= end_dt LOOP
        date_id := TO_CHAR(dt, 'YYYYMMDD')::BIGINT;
        fiscal_year := EXTRACT(YEAR FROM dt)::INT;
        fiscal_month := EXTRACT(MONTH FROM dt)::INT;
        fiscal_month_name := '04-' || TRIM(TO_CHAR(dt, 'Month'));
        calendar_year := fiscal_year;
        calendar_month := fiscal_month;
        calendar_month_name := '01-' || TRIM(TO_CHAR(dt, 'Month'));
        month_ending := (DATE_TRUNC('month', dt) + INTERVAL '1 month - 1 day')::DATE;

        INSERT INTO bl_dm.dim_date (
            date_id,
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
            date_id,
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
