SELECT num.adsh
FROM {{ ref('raw_num_table') }} num
LEFT JOIN {{ ref('raw_sub_table') }} sub ON num.adsh = sub.adsh
WHERE sub.adsh IS NULL
