-- ============================================
--  0. Create the table and fill with data
-- ============================================

--  Create the table 'table_to_delete' and fill it with data:
CREATE TABLE table_to_delete AS
SELECT 'veeeeeeery_long_string' || x AS col
FROM generate_series(1, (10^7)::int) x;

-- Explanation: 
-- The `generate_series(1, 10^7)` function generates 10 million rows, with each row containing a string value created by concatenating 
-- 'veeeeeeery_long_string' with the row number. 
-- The table 'table_to_delete' will now have 10 million rows.

-- Execution time: 29 seconds


-- ===================================================
-- 1. Space Consumption Before DELETE
-- ===================================================

-- Query that shows space consumption before DELETE
SELECT *, 
       pg_size_pretty(total_bytes) AS total,
       pg_size_pretty(index_bytes) AS index,
       pg_size_pretty(toast_bytes) AS toast,
       pg_size_pretty(table_bytes) AS table
FROM (
    SELECT *, total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
    FROM (
        SELECT c.oid,
               nspname AS table_schema,
               relname AS table_name,
               c.reltuples AS row_estimate,
               pg_total_relation_size(c.oid) AS total_bytes,
               pg_indexes_size(c.oid) AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes
        FROM pg_class c
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE relkind = 'r'
    ) a
) a
WHERE table_name LIKE '%table_to_delete%';

-- Result: 575 MB (initial size before DELETE)


-- ===================================================
-- 2. DELETE Operation with Duration Measurement
-- ===================================================

EXPLAIN ANALYZE
DELETE FROM table_to_delete
WHERE REPLACE(col, 'veeeeeeery_long_string', '')::int % 3 = 0;

-- Expected result: DELETE takes ~20.315548 seconds


-- ===================================================
-- 3. Space Consumption After DELETE
-- ===================================================

SELECT *, 
       pg_size_pretty(total_bytes) AS total,
       pg_size_pretty(index_bytes) AS index,
       pg_size_pretty(toast_bytes) AS toast,
       pg_size_pretty(table_bytes) AS table
FROM (
    SELECT *, total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
    FROM (
        SELECT c.oid,
               nspname AS table_schema,
               relname AS table_name,
               c.reltuples AS row_estimate,
               pg_total_relation_size(c.oid) AS total_bytes,
               pg_indexes_size(c.oid) AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes
        FROM pg_class c
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE relkind = 'r'
    ) a
) a
WHERE table_name LIKE '%table_to_delete%';

-- Expected result: 575 MB (may not show much reduction yet).
-- Conclusion: DELETE marks rows as dead but does not reclaim space immediately.


-- ===================================================
-- 4. VACUUM FULL Operation with Duration Measurement
-- ===================================================

VACUUM FULL VERBOSE table_to_delete;

-- Expected result: VACUUM takes ~8 seconds to run.
-- Conclusion: VACUUM FULL rewrites the entire table, removing dead tuples and compacting space.


-- ===================================================
-- 5. Space Consumption After VACUUM FULL
-- ===================================================

SELECT *, 
       pg_size_pretty(total_bytes) AS total,
       pg_size_pretty(index_bytes) AS index,
       pg_size_pretty(toast_bytes) AS toast,
       pg_size_pretty(table_bytes) AS table
FROM (
    SELECT *, total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
    FROM (
        SELECT c.oid,
               nspname AS table_schema,
               relname AS table_name,
               c.reltuples AS row_estimate,
               pg_total_relation_size(c.oid) AS total_bytes,
               pg_indexes_size(c.oid) AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes
        FROM pg_class c
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE relkind = 'r'
    ) a
) a
WHERE table_name LIKE '%table_to_delete%';

-- Expected result: After VACUUM FULL, the table size should be reduced (e.g., 383 MB).
-- Conclusion: Significant space is recovered after VACUUM FULL.


-- ===================================================
-- 6. TRUNCATE Operation with Duration Measurement
-- ===================================================

TRUNCATE table_to_delete;

-- Expected result: TRUNCATE takes ~0.012 seconds.
-- Conclusion: TRUNCATE is extremely fast and removes all rows instantly, but is not transactional.


-- ===================================================
-- 7. Space Consumption After TRUNCATE
-- ===================================================

SELECT *, 
       pg_size_pretty(total_bytes) AS total,
       pg_size_pretty(index_bytes) AS index,
       pg_size_pretty(toast_bytes) AS toast,
       pg_size_pretty(table_bytes) AS table
FROM (
    SELECT *, total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
    FROM (
        SELECT c.oid,
               nspname AS table_schema,
               relname AS table_name,
               c.reltuples AS row_estimate,
               pg_total_relation_size(c.oid) AS total_bytes,
               pg_indexes_size(c.oid) AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes
        FROM pg_class c
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE relkind = 'r'
    ) a
) a
WHERE table_name LIKE '%table_to_delete%';

-- Expected result: After TRUNCATE, the table should occupy ~0 bytes.
-- Conclusion: TRUNCATE completely clears and resets the storage footprint.


-- ===================================================
-- 8. Final conclusions
-- ===================================================

-- DELETE:
-- - Duration: ~20.3 seconds
-- - Space after: ~575 MB (no change)
-- - Used for selective deletion; doesn't free disk space immediately.

-- VACUUM FULL:
-- - Duration: ~8 seconds
-- - Space after: ~383 MB
-- - Reclaims space after DELETE, but locks table and rewrites data.

-- TRUNCATE:
-- - Duration: ~0.012 seconds
-- - Space after: ~0 bytes
-- - Best for full deletion. Fast and fully reclaims space.

-- Overall Recommendation:
-- Use DELETE + VACUUM FULL when selective row removal is required.
-- Use TRUNCATE when you want to remove all data quickly and efficiently.