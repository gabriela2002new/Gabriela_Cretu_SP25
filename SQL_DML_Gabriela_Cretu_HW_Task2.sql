-- 1. Create the table 'table_to_delete' and fill it with data:

CREATE TABLE table_to_delete AS
SELECT 'veeeeeeery_long_string' || x AS col
FROM generate_series(1, (10^7)::int) x;

-- Explanation: 
-- The `generate_series(1, 10^7)` function generates 10 million rows, with each row containing a string value created by concatenating 
-- 'veeeeeeery_long_string' with the row number. 
-- The table 'table_to_delete' will now have 10 million rows.

-- 2. Check how much space the table consumes:

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

-- Explanation:
-- This query checks the total space consumed by the 'table_to_delete' table. It includes:
-- - `total_bytes`: The total space the table occupies (including indexes and TOAST storage).
-- - `index_bytes`: The space consumed by the indexes.
-- - `toast_bytes`: The space used by large objects (if any).
-- - `table_bytes`: The space consumed by just the table data.
-- Expected result: The table 'table_to_delete' occupies around **575 MB** of disk space initially.

-- 3. Perform the DELETE operation:

DELETE FROM table_to_delete
WHERE REPLACE(col, 'veeeeeeery_long_string', '')::int % 3 = 0;

-- Explanation:
-- This DELETE statement removes rows where the value in 'col' (after replacing 'veeeeeeery_long_string' with an empty string) is divisible by 3. 
-- Since there are 10 million rows, approximately one-third of the rows are removed.
-- Expected result: The DELETE operation takes around **18 seconds** to execute.

-- 4. Check space consumption after DELETE:

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

-- Explanation:
-- Despite deleting rows, the table still occupies around **575 MB** of disk space. 
-- This happens because the space used by deleted rows is not immediately freed. PostgreSQL marks the space as reusable, but the actual space is still allocated.
-- Thus, **DELETE** does not immediately reduce disk usage.

-- 5. Perform the VACUUM FULL operation:

VACUUM FULL VERBOSE table_to_delete;

-- Explanation:
-- The `VACUUM FULL` command reorganizes the table and reclaims space that was freed by the DELETE operation.
-- It removes the unused space and physically shrinks the table, which can help reduce disk usage.
-- `VERBOSE` option provides detailed output on the vacuuming process.

-- 6. Check space consumption after VACUUM FULL:

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

-- Explanation:
-- After performing the `VACUUM FULL` operation, the table size is reduced to around **383 MB**. 
-- This is because `VACUUM FULL` reclaims unused space that was left by deleted rows, resulting in a more compact storage for the table.
-- The space used by the table is now reduced and more efficient.

-- 7. Recreate the table and perform the TRUNCATE operation:

DROP TABLE IF EXISTS table_to_delete;
CREATE TABLE table_to_delete AS
SELECT 'veeeeeeery_long_string' || x AS col
FROM generate_series(1, (10^7)::int) x;

-- Explanation:
-- Recreating the table 'table_to_delete' with the same data (10 million rows).

-- Perform TRUNCATE operation:

TRUNCATE table_to_delete;

-- Explanation:
-- The `TRUNCATE` operation removes all rows from the table **instantly**, unlike `DELETE`, which works row by row.
-- `TRUNCATE` is a much faster operation and does not generate individual row delete logs. This operation takes **0.035 seconds** to complete.

-- 8. Check space consumption after TRUNCATE:

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

-- Explanation:
-- After performing the `TRUNCATE` operation, the table should occupy **0 bytes** since it no longer contains any rows.
-- The `TRUNCATE` command quickly removes all rows, and PostgreSQL does not need to reclaim space as it does with `DELETE`.

*/