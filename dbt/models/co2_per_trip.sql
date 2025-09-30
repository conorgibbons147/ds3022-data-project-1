-- This model adds a trip_co2_kgs column to taxi trip tables
-- and calculates CO2 per trip using the emissions lookup table.

-- models/co2_per_trip.sql

{% set targets = [
  {"table": "yellow_tripdata_2024", "vehicle": "yellow_taxi"},
  {"table": "green_tripdata_2024",  "vehicle": "green_taxi"}
] %}

{% set emissions_rel = "main.vehicle_emissions" %}

{% set prehooks = [] %}
{% for t in targets %}
  {% do prehooks.append("ALTER TABLE " ~ t.table ~ " ADD COLUMN IF NOT EXISTS trip_co2_kgs DOUBLE") %}

  {% set upd %}
UPDATE {{ t.table }}
SET trip_co2_kgs = (
  trip_distance * (
    SELECT co2_grams_per_mile
    FROM {{ emissions_rel }}
    WHERE LOWER(vehicle_type) = '{{ t.vehicle }}'
    LIMIT 1
  )
) / 1000.0
  {% endset %}

  {% do prehooks.append(upd) %}
{% endfor %}

{{ config(materialized='view', pre_hook=prehooks) }}

{% for t in targets %}
  {{ log("Applied trip_co2_kgs update to " ~ t.table, info=True) }}
{% endfor %}

SELECT 1 AS applied


