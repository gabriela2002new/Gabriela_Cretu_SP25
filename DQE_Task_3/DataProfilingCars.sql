-- 1. Enable file_fdw extension
CREATE EXTENSION IF NOT EXISTS file_fdw;

-- 2. Create a server for CSV files
CREATE SERVER csv_server FOREIGN DATA WRAPPER file_fdw;

-- 3. Create foreign table for bank.csv
CREATE FOREIGN TABLE bank_external (
    age VARCHAR,
    job VARCHAR,
    marital VARCHAR,
    education VARCHAR,
    "default" VARCHAR,  -- <-- quotes around reserved word
    balance VARCHAR,
    housing VARCHAR,
    loan VARCHAR,
    contact VARCHAR,
    duration VARCHAR
)
SERVER csv_server
OPTIONS (
    filename 'C:/D into C(08-06-2025_17-38)/csv/bank.csv',
    format 'csv',
    header 'true'
);
  
--1. Negative or unrealistic ages  
SELECT * 
FROM bank_external
WHERE age::int < 18 OR age::int > 100;

--2.Check negative balances
SELECT *
FROM bank_external
WHERE balance::numeric < 0;

--3. Check unrealistic call durations 
SELECT *
FROM bank_external
WHERE duration::int <= 0
OR duration::int > 3600;  -- example threshold: > 1 hour

--4.Check categorial values
SELECT DISTINCT "default"
FROM bank_external
WHERE "default" NOT IN ('yes', 'no');

SELECT DISTINCT housing
FROM bank_external
WHERE housing NOT IN ('yes', 'no');

SELECT DISTINCT loan
FROM bank_external
WHERE loan NOT IN ('yes', 'no');

--5.Check missing values

SELECT 
    SUM(CASE WHEN age IS NULL OR age = '' THEN 1 ELSE 0 END) AS missing_age,
    SUM(CASE WHEN job IS NULL OR job = '' THEN 1 ELSE 0 END) AS missing_job,
    SUM(CASE WHEN marital IS NULL OR marital = '' THEN 1 ELSE 0 END) AS missing_marital,
    SUM(CASE WHEN education IS NULL OR education = '' THEN 1 ELSE 0 END) AS missing_education,
    SUM(CASE WHEN "default" IS NULL OR "default" = '' THEN 1 ELSE 0 END) AS missing_default,
    SUM(CASE WHEN balance IS NULL OR balance = '' THEN 1 ELSE 0 END) AS missing_balance,
    SUM(CASE WHEN housing IS NULL OR housing = '' THEN 1 ELSE 0 END) AS missing_housing,
    SUM(CASE WHEN loan IS NULL OR loan = '' THEN 1 ELSE 0 END) AS missing_loan,
    SUM(CASE WHEN contact IS NULL OR contact = '' THEN 1 ELSE 0 END) AS missing_contact,
    SUM(CASE WHEN duration IS NULL OR duration = '' THEN 1 ELSE 0 END) AS missing_duration
FROM bank_external;

--6. Check duplicates 

SELECT age, job, marital, education, "default", balance, housing, loan, contact, duration, COUNT(*) AS count_dup
FROM bank_external
GROUP BY age, job, marital, education, "default", balance, housing, loan, contact, duration
HAVING COUNT(*) > 1;


--7. Contact column data issues

SELECT DISTINCT contact
FROM bank_external;

--8. Education column data issues 
SELECT DISTINCT education 
FROM bank_external;

--9.Job data anomaly
SELECT DISTINCT job 
FROM bank_external;
--10.Marital status data anomly
SELECT DISTINCT marital 
FROM bank_external;

select *
from bank_external be ;

--11. Check for logic anomalies
SELECT *
FROM bank_external
WHERE (age::int < 18 AND job = 'retired')
   OR (age::int > 100 AND job = 'student');
  
