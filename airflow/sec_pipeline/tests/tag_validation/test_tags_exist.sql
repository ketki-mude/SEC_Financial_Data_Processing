SELECT tag
FROM {{ ref('raw_tag_table') }}
WHERE tag IS NULL
