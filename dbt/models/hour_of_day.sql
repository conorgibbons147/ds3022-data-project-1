-- This model adds an hour_of_day column to trip tables
-- and sets it by extracting the pickup hour from the timestamp.

{% set targets = [
  {"table": "yellow_tripdata_2024", "pick": "tpep_pickup_datetime"},
  {"table": "green_tripdata_2024",  "pick": "lpep_pickup_datetime"}
] %}

{# Build pre_hook list safely #}
{% set prehooks = [] %}
{% for t in targets %}
  {% do prehooks.append("ALTER TABLE " ~ t.table ~ " ADD COLUMN IF NOT EXISTS hour_of_day INTEGER") %}
  {% do prehooks.append("UPDATE " ~ t.table ~ " SET hour_of_day = EXTRACT(HOUR FROM " ~ t.pick ~ ")") %}
{% endfor %}

{{ config(
  materialized = 'view',
  pre_hook = prehooks
) }}

{% for t in targets %}
  {{ log("Applied hour_of_day update to " ~ t.table, info=True) }}
{% endfor %}

-- Dummy select so dbt considers this a valid model
SELECT 1 AS applied
