WITH duplicate_rows AS (
    SELECT adsh, report, line, COUNT(*) AS row_count
    FROM {{ ref('raw_pre_table') }}
    GROUP BY adsh, report, line
)
SELECT adsh, report, line, row_count 
FROM duplicate_rows
WHERE row_count > 1