{{ config(materialized='table') }}

SELECT
    CAST(game_id as STRING) as game_id,
    CAST(team_id as STRING) as team_id,
    CAST(team_points as INT64) as team_points,
    CAST(turnovers as INT64) as turnovers,
    CAST(rebounds as INT64) as rebounds,
    CAST(fg_percentage as FLOAT64) as fg_percentage,
    CAST(ft_percentage as FLOAT64) as ft_percentage,
    CAST(tp_percentage as FLOAT64) as tp_percentage
FROM {{ source('bronze', 'raw_team_game_stats') }}
