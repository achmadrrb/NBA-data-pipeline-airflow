{{ config(materialized='table') }}

SELECT
    CAST(team_id AS STRING) AS team_id,
    INITCAP(team_name) AS team_name,
    INITCAP(conference) AS conference,
    INITCAP(division) AS division,
    SAFE_CAST(founded_year AS INT64) AS founded_year,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('bronze', 'raw_teams') }}
