{{ config(materialized='table') }}

SELECT
    CAST(player_id AS STRING) AS player_id,
    INITCAP(player_name) AS player_name,
    SAFE_CAST(height_inches AS INT64) AS height_inches,
    SAFE_CAST(weight_lbs AS INT64) AS weight_lbs,
    SAFE_CAST(birth_date AS DATE) AS birth_date,
    SAFE_CAST(debut_year AS INT64) AS debut_year,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('bronze', 'raw_players') }}
