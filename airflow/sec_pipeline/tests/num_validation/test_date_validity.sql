SELECT DDATE
FROM {{ ref('raw_num_table') }}
WHERE 
    LENGTH(CAST(DDATE AS STRING)) != 8 
