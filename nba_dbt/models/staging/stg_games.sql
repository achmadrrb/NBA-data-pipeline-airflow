{{ config(materialized='table') }}

SELECT
    CAST(game_id AS STRING) AS game_id,
    CAST(date AS DATE) AS game_date,
    home_team,
    away_team,
    CAST(home_score AS INT64) AS home_score,
    CAST(away_score AS INT64) AS away_score,
    arena,
    attendance
FROM {{ source('bronze', 'raw_games') }}
