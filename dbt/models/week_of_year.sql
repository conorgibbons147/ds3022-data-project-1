-- This model adds a week_of_year column to trip tables
-- and sets it by extracting the ISO week number from pickup timestamps.

{% set targets = [
  {"table": "yellow_tripdata_2024", "pick": "tpep_pickup_datetime"},
  {"table": "green_tripdata_2024",  "pick": "lpep_pickup_datetime"}
] %}

{# Build the pre_hook statements in a Python list first #}
{% set prehooks = [] %}
{% for t in targets %}
  {% do prehooks.append("ALTER TABLE " ~ t.table ~ " ADD COLUMN IF NOT EXISTS week_of_year INTEGER") %}
  {% do prehooks.append("UPDATE " ~ t.table ~ " SET week_of_year = EXTRACT(WEEK FROM " ~ t.pick ~ ")") %}
{% endfor %}

{{ config(
  materialized = 'view',
  pre_hook = prehooks
) }}

{# Friendly logs #}
{% for t in targets %}
  {{ log("Applied week_of_year update to " ~ t.table, info=True) }}
{% endfor %}

-- Dummy select to keep dbt happy
SELECT 1 AS applied

