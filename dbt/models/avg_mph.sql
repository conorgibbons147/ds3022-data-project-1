
-- This model adds an avg_mph column to the yellow and green trip tables
-- and fills it by dividing distance by trip duration (in hours).

-- models/avg_mph.sql
-- depends_on: {{ ref('co2_per_trip') }}

{% set targets = [
  {"table": "yellow_tripdata_2024", "pick": "tpep_pickup_datetime", "drop": "tpep_dropoff_datetime"},
  {"table": "green_tripdata_2024",  "pick": "lpep_pickup_datetime", "drop": "lpep_dropoff_datetime"}
] %}

{% set prehooks = [] %}
{% for t in targets %}
  {% do prehooks.append("ALTER TABLE " ~ t.table ~ " ADD COLUMN IF NOT EXISTS avg_mph DOUBLE") %}
  {% do prehooks.append(
    "UPDATE " ~ t.table ~ " SET avg_mph = CASE " ~
    "WHEN datediff('second', " ~ t.pick ~ ", " ~ t.drop ~ ") > 0 " ~
      "THEN trip_distance / (datediff('second', " ~ t.pick ~ ", " ~ t.drop ~ ") / 3600.0) " ~
    "ELSE NULL END"
  ) %}
{% endfor %}

-- Run these updates before building the model, then just return a dummy row
{{ config(materialized='view', pre_hook=prehooks) }}

{% for t in targets %}
  {{ log("Applied avg_mph update to " ~ t.table, info=True) }}
{% endfor %}

SELECT 1 AS applied


