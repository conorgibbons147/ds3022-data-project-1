-- This model adds a day_of_week column to trip tables
-- and fills it by extracting the weekday number from pickup timestamps.

{% set targets = [
  {"table": "yellow_tripdata_2024", "pick": "tpep_pickup_datetime"},
  {"table": "green_tripdata_2024",  "pick": "lpep_pickup_datetime"}
] %}

{# Build pre_hook list safely #}
{% set prehooks = [] %}
{% for t in targets %}
  {% do prehooks.append("ALTER TABLE " ~ t.table ~ " ADD COLUMN IF NOT EXISTS day_of_week INTEGER") %}
  {% do prehooks.append("UPDATE " ~ t.table ~ " SET day_of_week = EXTRACT(DAYOFWEEK FROM " ~ t.pick ~ ")") %}
{% endfor %}

{{ config(
  materialized = 'view',
  pre_hook = prehooks
) }}

{% for t in targets %}
  {{ log("Applied day_of_week update to " ~ t.table, info=True) }}
{% endfor %}

-- Dummy select so dbt considers this model valid
SELECT 1 AS applied
