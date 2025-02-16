{% set table_name = 'RAW_SUB_' ~ var('file_name', '2023Q1') %}

{{ config(
    alias=table_name  
) }}

SELECT
    CAST(NULL AS VARCHAR(20)) AS adsh,
    CAST(NULL AS NUMBER(38,0)) AS cik,
    CAST(NULL AS VARCHAR(150)) AS name,
    CAST(NULL AS NUMBER(38,0)) AS sic,
    CAST(NULL AS CHAR(2)) AS countryba,
    CAST(NULL AS CHAR(2)) AS stprba,
    CAST(NULL AS VARCHAR(30)) AS cityba,
    CAST(NULL AS VARCHAR(10)) AS zipba,
    CAST(NULL AS VARCHAR(40)) AS bas1,
    CAST(NULL AS VARCHAR(40)) AS bas2,
    CAST(NULL AS VARCHAR(20)) AS baph,
    CAST(NULL AS CHAR(2)) AS countryma,
    CAST(NULL AS CHAR(2)) AS stprma,
    CAST(NULL AS VARCHAR(30)) AS cityma,
    CAST(NULL AS VARCHAR(10)) AS zipma,
    CAST(NULL AS VARCHAR(40)) AS mas1,
    CAST(NULL AS VARCHAR(40)) AS mas2,
    CAST(NULL AS CHAR(3)) AS countryinc,
    CAST(NULL AS CHAR(2)) AS stprinc,
    CAST(NULL AS NUMBER(38,0)) AS ein,
    CAST(NULL AS VARCHAR(150)) AS former,
    CAST(NULL AS NUMBER(38,0)) AS changed,
    CAST(NULL AS VARCHAR(5)) AS afs,
    CAST(NULL AS NUMBER(1,0)) AS wksi,
    CAST(NULL AS NUMBER(38,0)) AS fye,
    CAST(NULL AS VARCHAR(10)) AS form,
    CAST(NULL AS NUMBER(38,0)) AS period,
    CAST(NULL AS NUMBER(38,0)) AS fy,
    CAST(NULL AS VARCHAR(2)) AS fp,
    CAST(NULL AS NUMBER(38,0)) AS filed,
    CAST(NULL AS VARCHAR(30)) AS accepted,
    CAST(NULL AS NUMBER(1,0)) AS prevrpt,
    CAST(NULL AS NUMBER(1,0)) AS detail,
    CAST(NULL AS VARCHAR(40)) AS instance,
    CAST(NULL AS NUMBER(38,0)) AS nciks,
    CAST(NULL AS VARCHAR(120)) AS aciks,
    CAST(NULL AS VARCHAR(20)) AS source_file
WHERE FALSE
