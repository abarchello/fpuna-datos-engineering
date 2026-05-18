{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'pokemon') }}
),

renamed as (
    select
        id                                          as pokemon_id,
        name                                        as pokemon_name,
        height,
        weight,
        base_experience,
        types,
        cast(height as double) / 10.0               as height_meters,
        cast(weight as double) / 10.0               as weight_kg,
        is_default,
        "order"                                     as pokedex_order,

        -- Stats (anidadas en JSON en PokeAPI)
        try_cast(stats[1].base_stat as integer)     as hp,
        try_cast(stats[2].base_stat as integer)     as attack,
        try_cast(stats[3].base_stat as integer)     as defense,
        try_cast(stats[4].base_stat as integer)     as special_attack,
        try_cast(stats[5].base_stat as integer)     as special_defense,
        try_cast(stats[6].base_stat as integer)     as speed,

        -- Tipo principal (cast explícito a VARCHAR para evitar tipo JSON de DuckDB)
        CAST(types[1].type.name AS VARCHAR)         as primary_type,
        CAST(types[2].type.name AS VARCHAR)         as secondary_type,

        current_timestamp                           as loaded_at

    from source
)

select * from renamed
