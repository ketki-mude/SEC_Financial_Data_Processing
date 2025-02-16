SELECT crdr
FROM {{ ref('raw_tag_table') }}
WHERE crdr NOT IN ('C', 'D')
