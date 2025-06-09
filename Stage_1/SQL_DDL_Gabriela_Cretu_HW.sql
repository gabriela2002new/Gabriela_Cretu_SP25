/*drop tables if you want to rerun the code */
-- Drop join and dependent tables first
DROP TABLE IF EXISTS recruitment_data.Skill_Listing CASCADE;
DROP TABLE IF EXISTS recruitment_data.Preference_Candidate CASCADE;
DROP TABLE IF EXISTS recruitment_data.Skill_Candidate CASCADE;
DROP TABLE IF EXISTS recruitment_data.Preference CASCADE;
DROP TABLE IF EXISTS recruitment_data.Skill CASCADE;
DROP TABLE IF EXISTS recruitment_data.Payment CASCADE;
DROP TABLE IF EXISTS recruitment_data.Placement CASCADE;
DROP TABLE IF EXISTS recruitment_data.Interview CASCADE;
DROP TABLE IF EXISTS recruitment_data.Application CASCADE;
DROP TABLE IF EXISTS recruitment_data.Job_Listing CASCADE;
DROP TABLE IF EXISTS recruitment_data.Recruiter CASCADE;
DROP TABLE IF EXISTS recruitment_data.Company CASCADE;
DROP TABLE IF EXISTS recruitment_data.job_alert CASCADE;
DROP TABLE IF EXISTS recruitment_data.Service CASCADE;
DROP TABLE IF EXISTS recruitment_data.Candidate CASCADE;

-- Drop ENUM types (only after all tables using them are dropped)
DROP TYPE IF EXISTS skill_level_enum;
DROP TYPE IF EXISTS preference_level_enum;
DROP TYPE IF EXISTS job_type_pref_enum;
DROP TYPE IF EXISTS payment_status_enum;
DROP TYPE IF EXISTS interview_status_enum;
DROP TYPE IF EXISTS application_status_enum;
DROP TYPE IF EXISTS job_status_enum;
DROP TYPE IF EXISTS job_type_enum;
DROP TYPE IF EXISTS alert_status_enum;
DROP TYPE IF EXISTS frequency_enum;
DROP TYPE IF EXISTS service_status_enum;
DROP TYPE IF EXISTS service_type_enum;





/*
Create a physical database with a separate database and schema and give it an appropriate domain-related name. Use the relational model 
you've created while studying DB Basics module. Task 2 (designing a logical data model on the chosen topic). Make sure you have made any 
changes to your model after your mentor's comments.
*/

CREATE DATABASE RecruitmentAgency;
create schema recruitment_data;
/*Your database must be in 3NF*/
/*Use appropriate data types for each column and apply DEFAULT values, and GENERATED ALWAYS AS columns as required.*/
/*Create relationships between tables using primary and foreign keys.*/

/*Apply five check constraints across the tables to restrict certain values, including

--date to be inserted, which must be greater than January 1, 2000
--inserted measured value that cannot be negative
--inserted value that can only be a specific value (as an example of gender)
--unique
--not null

*/
--1.Candidate table

CREATE TABLE recruitment_data.Candidate (
    candidate_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    name VARCHAR(101) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    email VARCHAR(150) NOT NULL UNIQUE,
    phone VARCHAR(20) UNIQUE,
    experience INT CHECK (experience >= 0),
    location VARCHAR(100),
    resume VARCHAR(255)
);
--2.Service Table

-- Step 1: Create ENUM types
CREATE TYPE service_type_enum AS ENUM ('Resume Writing', 'Interview Coaching', 'Skills Development');
CREATE TYPE service_status_enum AS ENUM ('Pending', 'Completed', 'In Progress'); -- optional

-- Step 2: Create the Service table using REFERENCES for candidate_id
CREATE TABLE recruitment_data.Service (
    service_id SERIAL PRIMARY KEY,
    candidate_id INT NOT NULL REFERENCES recruitment_data.Candidate(candidate_id) ON DELETE CASCADE,
    service_type service_type_enum NOT NULL,
    service_status service_status_enum NOT NULL DEFAULT 'Pending'
);


--3.Job Alert

-- Step 1: Create ENUM types for frequency and alert_status
CREATE TYPE frequency_enum AS ENUM ('Daily', 'Weekly');
CREATE TYPE alert_status_enum AS ENUM ('Active', 'Inactive');

-- Step 2: Create the Job_alert table with the conditional frequency logic
CREATE TABLE recruitment_data.job_alert (
    alert_id SERIAL PRIMARY KEY,
    candidate_id INT NOT NULL REFERENCES recruitment_data.candidate(candidate_id) ON DELETE CASCADE,
    job_category VARCHAR(20) NOT NULL,  
    frequency frequency_enum,  -- No NOT NULL, we will handle it conditionally
    alert_status alert_status_enum NOT NULL DEFAULT 'Active',

    -- Check constraint to ensure frequency is NULL if alert_status is Inactive
    CONSTRAINT check_frequency_when_inactive CHECK (
        (alert_status = 'Active' AND frequency IS NOT NULL) OR
        (alert_status = 'Inactive' AND frequency IS NULL))
);

--4.Company

-- Step 1: Create the Company table
CREATE TABLE recruitment_data.Company (
    company_id SERIAL PRIMARY KEY,  -- Primary key with auto-increment
    company_name VARCHAR(40) NOT NULL,  -- Name of the company (adjust length as needed)
    company_industry VARCHAR(40),  -- Industry of the company
    headquarters VARCHAR(40),  -- Location of the company's headquarters
    contact_email VARCHAR(40) UNIQUE NOT NULL,  -- Email address with a uniqueness constraint
    contact_phone VARCHAR(20) UNIQUE,  -- Phone number with a uniqueness constraint
    
    -- Optional: Check for valid phone format (only digits and optional "+" prefix)
    CONSTRAINT check_phone_format CHECK (contact_phone ~ '^\+?[0-9]{10,15}$')
);

--5.Recruiter
-- Step 1: Create the Recruiter table with a foreign key referencing the Company table
CREATE TABLE recruitment_data.Recruiter (
    recruiter_id SERIAL PRIMARY KEY,
    company_id INT NOT NULL REFERENCES recruitment_data.Company(company_id) ON DELETE CASCADE,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    recruiter_name VARCHAR(101) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    recruiter_email VARCHAR(40) UNIQUE NOT NULL,
    recruiter_phone VARCHAR(20) UNIQUE,
    CONSTRAINT check_phone_format CHECK (recruiter_phone ~ '^\+?[0-9]{10,15}$')
);

--6.Job_Listing
-- Step 1: Create ENUM types for job_type and job_status
CREATE TYPE job_type_enum AS ENUM ('Part-time', 'Full-time', 'Remote');
CREATE TYPE job_status_enum AS ENUM ('Open', 'Closed', 'Filled');

-- Step 2: Create the Job_Listing table with foreign keys referencing Company and Recruiter tables
CREATE TABLE recruitment_data.Job_Listing (
    job_id SERIAL PRIMARY KEY,  -- Primary key with auto-increment
    company_id INT NOT NULL REFERENCES recruitment_data.Company(company_id) ON DELETE CASCADE,  -- Foreign key referencing Company table
    recruiter_id INT NOT NULL REFERENCES recruitment_data.Recruiter(recruiter_id) ON DELETE CASCADE,  -- Foreign key referencing Recruiter table
    title VARCHAR(255) NOT NULL,  -- Job title
    description TEXT,  -- Job description (TEXT is used as it can hold longer content)
    job_location VARCHAR(255),  -- Job location
    posted_date DATE NOT NULL DEFAULT CURRENT_DATE,  -- Date when the job was posted
    experience_required INT,  -- Required experience in years
    job_type job_type_enum,  -- Job type (ENUM)
    job_status job_status_enum NOT NULL DEFAULT 'Open',  -- Job status (ENUM)
    CONSTRAINT check_posted_date CHECK (posted_date > '2000-01-01')  -- Adding the date constraint

    
);

--7.Application

-- Step 1: Create ENUM type for application_status
CREATE TYPE application_status_enum AS ENUM ('Applied', 'Under Review', 'Rejected', 'Hired');

-- Step 2: Create the Application table with foreign keys referencing Candidate and Job_Listing tables
CREATE TABLE recruitment_data.Application (
    application_id SERIAL PRIMARY KEY,  -- Primary key with auto-increment
    candidate_id INT NOT NULL REFERENCES recruitment_data.Candidate(candidate_id) ON DELETE CASCADE,  -- Foreign key referencing Candidate table
    job_id INT NOT NULL REFERENCES recruitment_data.Job_Listing(job_id) ON DELETE CASCADE,  -- Foreign key referencing Job_Listing table
    application_status application_status_enum NOT NULL,  -- Status of the application (ENUM)
    applied_date DATE NOT NULL DEFAULT CURRENT_DATE,  -- Date when the application was submitted
        CONSTRAINT check_applied_date CHECK (applied_date > '2000-01-01')  -- Adding the date constraint

);

--8.Interview

-- Step 1: Create ENUM type for interview_status
CREATE TYPE interview_status_enum AS ENUM ('Scheduled', 'Completed', 'Cancelled');

-- Step 2: Create the Interview table with foreign keys referencing Candidate and Job_Listing tables
CREATE TABLE recruitment_data.Interview (
    interview_id SERIAL PRIMARY KEY,  -- Primary key with auto-increment
    candidate_id INT NOT NULL REFERENCES recruitment_data.Candidate(candidate_id) ON DELETE CASCADE,  -- Foreign key referencing Candidate table
    job_id INT NOT NULL REFERENCES recruitment_data.Job_Listing(job_id) ON DELETE CASCADE,  -- Foreign key referencing Job_Listing table
    interview_status interview_status_enum NOT NULL,  -- Status of the interview (ENUM)
    interview_date TIMESTAMP NOT NULL,  -- Date and time when the interview is scheduled
    feedback TEXT  -- Feedback provided after the interview (can be NULL)
        CONSTRAINT check_interview_date CHECK (interview_date > '2000-01-01')  -- Adding the date constraint

);

--9.Placement

-- Step 1: Create the Placement table with foreign keys referencing Candidate, Job_Listing, and Company tables
CREATE TABLE recruitment_data.Placement (
    placement_id SERIAL PRIMARY KEY,  -- Primary key with auto-increment
    candidate_id INT NOT NULL REFERENCES recruitment_data.Candidate(candidate_id) ON DELETE CASCADE,  -- Foreign key referencing Candidate table
    job_id INT NOT NULL REFERENCES recruitment_data.Job_Listing(job_id) ON DELETE CASCADE,  -- Foreign key referencing Job_Listing table
    company_id INT NOT NULL REFERENCES Company(company_id) ON DELETE CASCADE,  -- Foreign key referencing Company table
    placement_date DATE NOT NULL,  -- The date when the placement was made
    salary DECIMAL(15, 2) NOT NULL -- The salary offered for the position (with two decimal places)
        CONSTRAINT check_placement_date CHECK (placement_date > '2000-01-01')  -- Adding the date constraint


);

--10.Payment

-- Step 1: Create ENUM type for payment_status
CREATE TYPE payment_status_enum AS ENUM (
    'Pending',
    'Processing',
    'Completed',
    'Failed',
    'Cancelled',
    'Refunded'
);
-- Step 2: Create the Payment table
CREATE TABLE recruitment_data.Payment (
    payment_id SERIAL PRIMARY KEY,  -- Auto-increment primary key
    candidate_id INT NOT NULL REFERENCES recruitment_data.Candidate(candidate_id) ON DELETE CASCADE,
    service_id INT NOT NULL REFERENCES recruitment_data.Service(service_id) ON DELETE CASCADE,
    amount DECIMAL(10, 2) NOT NULL,  -- Two decimal places for currency
    payment_date DATE NOT NULL,
    payment_status payment_status_enum NOT null
        CONSTRAINT check_payment_date CHECK (payment_date > '2000-01-01')  -- Adding the date constraint


);

--11.Skill

CREATE TABLE recruitment_data.Skill (
    skill_id SERIAL PRIMARY KEY,
    skill_name VARCHAR(100) UNIQUE NOT NULL
);

--12.Preference

-- Step 1: Create ENUM type for preferred_job_type
CREATE TYPE job_type_pref_enum AS ENUM ('Part-Time', 'Full-time', 'Remote');

-- Step 2: Create the Preference table
CREATE TABLE recruitment_data.Preference (
    preference_id SERIAL PRIMARY KEY,
    desired_salary DECIMAL(10, 2),
    preferred_location VARCHAR(100),
    preferred_job_type job_type_pref_enum,
    preferred_industry VARCHAR(100)
);


--13.Skill_Candidate

-- Step 1: Create ENUM type for skill_level
CREATE TYPE skill_level_enum AS ENUM ('Beginner', 'Intermediate', 'Expert');

-- Step 2: Create the Skill_Candidate join table
CREATE TABLE recruitment_data.Skill_Candidate (
    candidate_id INT NOT NULL REFERENCES recruitment_data.Candidate(candidate_id) ON DELETE CASCADE,
    skill_id INT NOT NULL REFERENCES recruitment_data.Skill(skill_id) ON DELETE CASCADE,
    skill_level skill_level_enum NOT NULL,
    PRIMARY KEY (candidate_id, skill_id)
);

--14.Preference_Candidate

-- Step 1: Create ENUM type for preference_level
CREATE TYPE preference_level_enum AS ENUM ('highly satisfied', 'satisfied', 'standard');

-- Step 2: Create the Preference_Candidate join table
CREATE TABLE recruitment_data.Preference_Candidate (
    preference_id INT NOT NULL REFERENCES recruitment_data.Preference(preference_id) ON DELETE CASCADE,
    candidate_id INT NOT NULL REFERENCES recruitment_data.Candidate(candidate_id) ON DELETE CASCADE,
    preference_level preference_level_enum NOT NULL,
    PRIMARY KEY (preference_id, candidate_id)
);

--15.Skill Listing

-- Step 1: Reuse the ENUM type for skill_level if already created
-- If not already created, uncomment the line below:
-- CREATE TYPE skill_level_enum AS ENUM ('Beginner', 'Intermediate', 'Expert');

-- Step 2: Create the Skill_Listing table
CREATE TABLE recruitment_data.Skill_Listing (
    skill_id INT NOT NULL REFERENCES recruitment_data.Skill(skill_id) ON DELETE CASCADE,
    job_id INT NOT NULL REFERENCES recruitment_data.Job_Listing(job_id) ON DELETE CASCADE,
    skill_level_required skill_level_enum NOT NULL,
    PRIMARY KEY (skill_id, job_id)
);

/*Populate the tables with the sample data generated, ensuring each table has at least two rows (for a total of 20+ rows in all the tables).
*/

-- Candidate
INSERT INTO recruitment_data.Candidate (first_name, last_name, email, phone, experience, location, resume)
VALUES 
('Alice', 'Smith', 'alice.smith@example.com', '+12345678901', 3, 'New York', 'alice_resume.pdf'),
('Bob', 'Johnson', 'bob.johnson@example.com', '+12345678902', 5, 'Los Angeles', 'bob_resume.pdf');

-- Service
INSERT INTO recruitment_data.Service (candidate_id, service_type, service_status)
VALUES 
(1, 'Resume Writing', 'Completed'),
(2, 'Interview Coaching', 'In Progress');

-- Job Alert
INSERT INTO recruitment_data.job_alert (candidate_id, job_category, frequency, alert_status)
VALUES 
(1, 'Software', 'Daily', 'Active'),
(2, 'Design', NULL, 'Inactive');

-- Company
INSERT INTO recruitment_data.Company (company_name, company_industry, headquarters, contact_email, contact_phone)
VALUES 
('TechNova', 'Software', 'San Francisco', 'hr@technova.com', '+19876543210'),
('DesignPro', 'Creative', 'Chicago', 'hello@designpro.com', '+19876543211');

-- Recruiter
INSERT INTO recruitment_data.Recruiter (company_id, first_name, last_name, recruiter_email, recruiter_phone)
VALUES 
(1, 'John', 'Doe', 'john.doe@technova.com', '+19876543220'),
(2, 'Jane', 'Miller', 'jane.miller@designpro.com', '+19876543221');

-- Job Listing
INSERT INTO recruitment_data.Job_Listing (company_id, recruiter_id, title, description, job_location, experience_required, job_type)
VALUES 
(1, 1, 'Frontend Developer', 'React developer for UI work', 'Remote', 2, 'Remote'),
(2, 2, 'UX Designer', 'Design web & mobile interfaces', 'Chicago', 3, 'Full-time');

-- Application
INSERT INTO recruitment_data.Application (candidate_id, job_id, application_status)
VALUES 
(1, 1, 'Applied'),
(2, 2, 'Under Review');

-- Interview
INSERT INTO recruitment_data.Interview (candidate_id, job_id, interview_status, interview_date, feedback)
VALUES 
(1, 1, 'Scheduled', '2025-04-15 10:00:00', NULL),
(2, 2, 'Completed', '2025-03-28 14:00:00', 'Strong design skills.');

-- Placement
INSERT INTO recruitment_data.Placement (candidate_id, job_id, company_id, placement_date, salary)
VALUES 
(1, 1, 1, '2025-04-01', 75000.00),
(2, 2, 2, '2025-04-02', 68000.00);

-- Payment
INSERT INTO recruitment_data.Payment (candidate_id, service_id, amount, payment_date, payment_status)
VALUES 
(1, 1, 199.99, '2025-03-25', 'Completed'),
(2, 2, 149.99, '2025-03-26', 'Processing');

-- Skill
INSERT INTO recruitment_data.Skill (skill_name)
VALUES 
('JavaScript'),
('Figma');

-- Preference
INSERT INTO recruitment_data.Preference (desired_salary, preferred_location, preferred_job_type, preferred_industry)
VALUES 
(70000.00, 'Remote', 'Remote', 'Tech'),
(65000.00, 'Chicago', 'Full-time', 'Creative');

-- Skill_Candidate
INSERT INTO recruitment_data.Skill_Candidate (candidate_id, skill_id, skill_level)
VALUES 
(1, 1, 'Expert'),
(2, 2, 'Intermediate');

-- Preference_Candidate
INSERT INTO recruitment_data.Preference_Candidate (preference_id, candidate_id, preference_level)
VALUES 
(1, 1, 'highly satisfied'),
(2, 2, 'satisfied');

-- Skill_Listing
INSERT INTO recruitment_data.Skill_Listing (skill_id, job_id, skill_level_required)
VALUES 
(1, 1, 'Expert'),
(2, 2, 'Intermediate');



/*
Add a NOT NULL 'record_ts' field to each table using ALTER TABLE statements.
Set the DEFAULT value to CURRENT_DATE.
Check to make sure the value has been set for the existing rows.
*/

-- Add record_ts to all tables
ALTER TABLE recruitment_data.Candidate ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Service ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.job_alert ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Company ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Recruiter ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Job_Listing ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Application ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Interview ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Placement ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Payment ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Skill ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Preference ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Skill_Candidate ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Preference_Candidate ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE recruitment_data.Skill_Listing ADD COLUMN record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

---
select *
from candidate c ;

