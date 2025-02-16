SELECT adsh
FROM {{ ref('raw_num_table') }}
WHERE adsh IS NULL
