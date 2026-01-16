-- Staging: content / catalog
-- Source : staging.stg_content
-- Nettoyage léger + renommage cohérent

{{ 
    config(
        materialized='view',
        tags=['staging', 'content']
    ) 
}}

WITH source AS (
    SELECT * 
    FROM {{ source('staging', 'stg_content') }}
),

cleaned AS (
    SELECT
        id AS content_id,
        title,
        content_type,
        genre,
        subgenre,
        release_year,
        duration_minutes,
        director,
        main_actor,
        imdb_rating,
        content_rating,
        is_original,
        added_date,
        available_countries,
        tags,
        description,
        _loaded_at
    FROM source
)

SELECT * FROM cleaned
