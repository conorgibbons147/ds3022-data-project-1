-- This model adds a month_of_year column to trip tables
-- and fills it by extracting the month number from pickup timestamps.

{% set targets = [
  {"table": "yellow_tripdata_2024", "pick": "tpep_pickup_datetime"},
  {"table": "green_tripdata_2024",  "pick": "lpep_pickup_datetime"}
] %}

{# Build pre_hook list safely #}
{% set prehooks = [] %}
{% for t in targets %}
  {% do prehooks.append("ALTER TABLE " ~ t.table ~ " ADD COLUMN IF NOT EXISTS month_of_year INTEGER") %}
  {% do prehooks.append("UPDATE " ~ t.table ~ " SET month_of_year = EXTRACT(MONTH FROM " ~ t.pick ~ ")") %}
{% endfor %}

{{ config(
  materialized = 'view',
  pre_hook = prehooks
) }}

{% for t in targets %}
  {{ log("Applied month_of_year update to " ~ t.table, info=True) }}
{% endfor %}

-- Dummy select so dbt considers this a valid model
SELECT 1 AS applied

