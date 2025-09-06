{{ config(materialized='table') }}

SELECT
    CAST(game_id AS STRING) AS game_id,
    CAST(player_id AS STRING) AS player_id,
    CAST(team_id AS STRING) AS team_id,
    SAFE_CAST(points AS INT64) AS points,
    SAFE_CAST(rebounds AS INT64) AS rebounds,
    SAFE_CAST(assists AS INT64) AS assists,
    SAFE_CAST(minutes AS FLOAT64) AS minutes_played,
    SAFE_CAST(field_goal_pct AS FLOAT64) / 100.0 AS field_goal_pct, -- 46 → 0.46
    PARSE_DATE('%Y-%m-%d', game_date) AS game_date,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('bronze', 'raw_player_game_stats') }}
