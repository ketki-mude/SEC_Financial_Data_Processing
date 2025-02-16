{% set table_name = 'RAW_TAG_' ~ var('file_name', '2023Q1') %}

{{ config(
    alias=table_name  
) }}

SELECT
    CAST(NULL AS VARCHAR(256)) AS tag,
    CAST(NULL AS VARCHAR(20)) AS version,
    CAST(NULL AS NUMBER(1,0)) AS custom,
    CAST(NULL AS NUMBER(1,0)) AS abstract,
    CAST(NULL AS VARCHAR(20)) AS datatype,
    CAST(NULL AS CHAR(1)) AS iord,
    CAST(NULL AS CHAR(1)) AS crdr,
    CAST(NULL AS VARCHAR(512)) AS tlabel,
    CAST(NULL AS TEXT) AS doc,
    CAST(NULL AS VARCHAR(20)) AS source_file
WHERE FALSE
