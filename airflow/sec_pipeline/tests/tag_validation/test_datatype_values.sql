SELECT datatype
FROM {{ ref('raw_tag_table') }}
WHERE datatype NOT IN (
    'monetary', 'shares', 'perShare', 'percent', 'integer', 'decimal', 
    'area', 'pure', 'mass', 'monetaryPerVolume'
) AND datatype IS NOT NULL
