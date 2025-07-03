CREATE OR REPLACE FUNCTION eomonth(date_in DATE) RETURNS DATE AS $$
BEGIN
    RETURN (date_trunc('month', date_in) + INTERVAL '1 month' - INTERVAL '1 day')::DATE;
END;
$$ LANGUAGE plpgsql IMMUTABLE strict;


drop table DimDate;
CREATE TABLE IF NOT EXISTS DimDate (
    DATE_ID INT PRIMARY KEY,
    EVENT_DT DATE,
    FISCAL_YEAR INT,
    FISCAL_MONTH INT,
    FISCAL_MONTH_NAME_NUMBER VARCHAR(50),
    CALENDAR_YEAR INT,
    CALENDAR_MONTH INT,
    CALENDAR_MONTH_NAME_NUMBER VARCHAR(50),
    MONTH_ENDING_DATE DATE,
    DAY_OF_WEEK INT,
    DAY_NAME VARCHAR(10),
    DAY_OF_MONTH INT,
    DAY_OF_YEAR INT,
    WEEK_OF_YEAR INT,
    QUARTER INT,
    IS_WEEKEND BOOLEAN
);



DO
$$
DECLARE
    dt DATE := '2023-01-01';
    end_date DATE := '2024-12-31';
    fiscal_month_start_day INT := 4;
    date_key INT;
    month_name TEXT;
    calendar_month_name_number TEXT;
    fiscal_month_name_number TEXT;
    month_end DATE;
    day_of_week INT;
    day_name TEXT;
    day_of_month INT;
    day_of_year INT;
    week_of_year INT;
    quarter INT;
    is_weekend BOOLEAN;
BEGIN
    WHILE dt <= end_date LOOP
        date_key := CAST(TO_CHAR(dt, 'YYYYMMDD') AS INT);
        month_name := TO_CHAR(dt, 'Month');
        month_name := TRIM(month_name);
        calendar_month_name_number := month_name || ' - 01';
        fiscal_month_name_number := month_name || ' - ' || LPAD(fiscal_month_start_day::TEXT, 2, '0');
        month_end:= eomonth(dt);
        day_of_week := EXTRACT(DOW FROM dt)::INT;  -- 0=Sunday .. 6=Saturday
        day_name := TO_CHAR(dt, 'Day');
        day_name := TRIM(day_name);
        day_of_month := EXTRACT(DAY FROM dt)::INT;
        day_of_year := EXTRACT(DOY FROM dt)::INT;
        week_of_year := EXTRACT(WEEK FROM dt)::INT;
        quarter := EXTRACT(QUARTER FROM dt)::INT;
        is_weekend := day_of_week IN (0,6);

        INSERT INTO DimDate (
            DATE_ID,
            EVENT_DT,
            FISCAL_YEAR,
            FISCAL_MONTH,
            FISCAL_MONTH_NAME_NUMBER,
            CALENDAR_YEAR,
            CALENDAR_MONTH,
            CALENDAR_MONTH_NAME_NUMBER,
            MONTH_ENDING_DATE,
            DAY_OF_WEEK,
            DAY_NAME,
            DAY_OF_MONTH,
            DAY_OF_YEAR,
            WEEK_OF_YEAR,
            QUARTER,
            IS_WEEKEND
        )
        VALUES (
            date_key,
            dt,
            EXTRACT(YEAR FROM dt)::INT,
            EXTRACT(MONTH FROM dt)::INT,
            fiscal_month_name_number,
            EXTRACT(YEAR FROM dt)::INT,
            EXTRACT(MONTH FROM dt)::INT,
            calendar_month_name_number,
            month_end,
            day_of_week + 1, -- Sunday=1 to Saturday=7
            day_name,
            day_of_month,
            day_of_year,
            week_of_year,
            quarter,
            is_weekend
        );

        dt := dt + INTERVAL '1 day';
    END LOOP;
END;
$$;

select *
from DimDate;