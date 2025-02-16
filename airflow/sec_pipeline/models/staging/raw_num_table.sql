{% set table_name = 'RAW_NUM_' ~ var('file_name', '2023Q1') %}

{{ config(
    alias=table_name  
) }}

SELECT
    CAST(NULL AS VARCHAR(20)) AS adsh,
    CAST(NULL AS VARCHAR(256)) AS tag,
    CAST(NULL AS VARCHAR(20)) AS version,
    CAST(NULL AS NUMBER(8,0)) AS ddate,
    CAST(NULL AS NUMBER(38,0)) AS qtrs,
    CAST(NULL AS VARCHAR(20)) AS uom,
    CAST(NULL AS STRING) AS segments,
    CAST(NULL AS VARCHAR(256)) AS coreg,
    CAST(NULL AS NUMBER(38,10)) AS value,
    CAST(NULL AS VARCHAR(512)) AS footnote,
    CAST(NULL AS VARCHAR(20)) AS source_file
WHERE FALSE
