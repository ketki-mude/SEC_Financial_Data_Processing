{% macro create_schema(schema_name) %}
  {% set sql %}
      CREATE SCHEMA IF NOT EXISTS {{ schema_name }};
  {% endset %}

  -- Log the query for debugging
  {% do log("Executing SQL: " ~ sql, info=True) %}

  -- Execute the query
  {% do run_query(sql) %}
{% endmacro %}