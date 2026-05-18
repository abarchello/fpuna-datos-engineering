{{ config(materialized='table') }}

-- OBT: One Big Table con todas las metricas de Pokemon
with base as (
    select
        pokemon_id,
        pokemon_name,
        primary_type,
        secondary_type,
        height_meters,
        weight_kg,
        base_experience,
        hp,
        attack,
        defense,
        special_attack,
        special_defense,
        speed,
        pokedex_order,
        is_default

    from {{ ref('stg_pokemon') }}
),

enriched as (
    select
        pokemon_id,
        pokemon_name,
        primary_type,
        coalesce(secondary_type, 'none')                        as secondary_type,
        height_meters,
        weight_kg,
        base_experience,
        hp,
        attack,
        defense,
        special_attack,
        special_defense,
        speed,
        pokedex_order,
        is_default,

        -- Metricas calculadas
        hp + attack + defense + special_attack + special_defense + speed  as total_base_stats,
        round(weight_kg / nullif(height_meters * height_meters, 0), 2)   as bmi_index,
        attack + special_attack                                           as total_attack_power,
        defense + special_defense                                         as total_defense_power,

        -- Clasificaciones
        case
            when hp + attack + defense + special_attack + special_defense + speed >= 600 then 'legendary_tier'
            when hp + attack + defense + special_attack + special_defense + speed >= 500 then 'strong'
            when hp + attack + defense + special_attack + special_defense + speed >= 400 then 'average'
            else 'weak'
        end                                                               as power_tier,

        case
            when attack > special_attack then 'physical_attacker'
            when special_attack > attack then 'special_attacker'
            else 'balanced'
        end                                                               as attack_style

    from base
)

select * from enriched
order by total_base_stats desc
