-- ============================================================================
-- PostgreSQL Query Scripts for Satisfaction Survey Data
-- ============================================================================

-- 1. List all tables in the database
-- ----------------------------------------------------------------------------
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
ORDER BY table_name;


-- 2. View basic info about the main cleaned data table
-- ----------------------------------------------------------------------------
-- Count rows
SELECT COUNT(*) as total_rows 
FROM satisfaction_2016_cleaned;

-- View first 10 rows
SELECT * 
FROM satisfaction_2016_cleaned 
LIMIT 10;

-- View column names and types
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'satisfaction_2016_cleaned' 
ORDER BY ordinal_position;


-- 3. Query the readable view (with Hebrew column aliases)
-- ----------------------------------------------------------------------------
-- Count rows
SELECT COUNT(*) as total_rows 
FROM vw_satisfaction_readable;

-- View first 10 rows with readable headers
SELECT * 
FROM vw_satisfaction_readable 
LIMIT 10;

-- View specific readable columns (example with a few key questions)
SELECT 
    code_hospital,
    "q3__אנא דרג/י את שביעות רצונך הכללית מהאשפוז במחלקה על סולם של 1...",
    "q31__אם חלילה יהיה צורך, האם תמליץ/י לחברים ולקרובי משפחה להתאשפז..."
FROM vw_satisfaction_readable 
LIMIT 10;


-- 4. Query hospital scores (aggregated data)
-- ----------------------------------------------------------------------------
-- View all hospital scores
SELECT * 
FROM hospital_scores 
ORDER BY code_hospital;

-- View hospitals with highest overall average
SELECT 
    code_hospital,
    overall_average
FROM hospital_scores 
ORDER BY overall_average DESC 
LIMIT 10;


-- 5. Query question metadata (question code to Hebrew text mapping)
-- ----------------------------------------------------------------------------
-- View all question texts
SELECT * 
FROM question_texts 
ORDER BY question_number;

-- Find a specific question
SELECT * 
FROM question_texts 
WHERE question_number = 3;


-- 6. Sample analytical queries
-- ----------------------------------------------------------------------------

-- Average satisfaction by hospital (from main table)
SELECT 
    code_hospital,
    AVG(q3) as avg_overall_satisfaction,
    COUNT(*) as response_count
FROM satisfaction_2016_cleaned 
WHERE q3 IS NOT NULL
GROUP BY code_hospital 
ORDER BY avg_overall_satisfaction DESC;

-- Compare question 3 and question 31 averages by hospital
SELECT 
    code_hospital,
    AVG(q3) as q3_avg,
    AVG(q31) as q31_avg,
    COUNT(*) as responses
FROM satisfaction_2016_cleaned 
WHERE q3 IS NOT NULL AND q31 IS NOT NULL
GROUP BY code_hospital 
ORDER BY q3_avg DESC;


-- ============================================================================
-- Usage Instructions:
-- ============================================================================
-- 1. Connect to PostgreSQL:
--    psql -h localhost -p 5433 -U postgres -d satisfaction
--
-- 2. Run a specific query by copying and pasting it into psql
--
-- 3. Or run the entire file:
--    psql -h localhost -p 5433 -U postgres -d satisfaction -f scripts/query_postgres.sql
--
-- ============================================================================
