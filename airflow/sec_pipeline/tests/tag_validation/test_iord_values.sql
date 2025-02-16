SELECT iord
FROM {{ ref('raw_tag_table') }}
WHERE iord NOT IN ('I', 'D')
AND iord IS NOT NULL
