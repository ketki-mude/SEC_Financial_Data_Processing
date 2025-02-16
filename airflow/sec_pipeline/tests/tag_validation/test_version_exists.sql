SELECT *
FROM {{ ref('raw_tag_table') }}
WHERE version IS NULL
