{% set table_name = 'RAW_PRE_' ~ var('file_name', '2023Q1') %}

{{ config(
    alias=table_name  
) }}

SELECT
    CAST(NULL AS VARCHAR(20)) AS adsh,
    CAST(NULL AS NUMBER(38,0)) AS report,
    CAST(NULL AS NUMBER(38,0)) AS line,
    CAST(NULL AS CHAR(2)) AS stmt,
    CAST(NULL AS NUMBER(1,0)) AS inpth,
    CAST(NULL AS CHAR(1)) AS rfile,
    CAST(NULL AS VARCHAR(256)) AS tag,
    CAST(NULL AS VARCHAR(20)) AS version,
    CAST(NULL AS VARCHAR(512)) AS plabel,
    CAST(NULL AS NUMBER(1,0)) AS negating,
    CAST(NULL AS VARCHAR(20)) AS source_file
WHERE FALSE