{% macro create_file_format() %}
  {% set sql %}
      USE SCHEMA {{ target.schema }};
      CREATE OR REPLACE FILE FORMAT parquet_format
      TYPE = PARQUET
      COMPRESSION = 'SNAPPY';
  {% endset %}

  -- Log the query for debugging
  {% do log("Executing SQL: " ~ sql, info=True) %}

  -- Execute the query
  {% do run_query(sql) %}
{% endmacro %}