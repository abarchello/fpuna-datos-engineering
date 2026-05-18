{{ config(materialized='view') }}

/*
  Modelo intermediate: int_hidro_diario
  -------------------------------------
  Agrega las mediciones a granularidad diaria por estación. Calcula
  promedios, mínimos, máximos y desviaciones para cada parámetro
  de calidad del agua.

  Capa: intermediate (preparación para marts)
*/

with mediciones as (
    select * from {{ ref('stg_hidro_mediciones') }}
),

diario as (
    select
        estacion,
        estacion_codigo,
        fecha_dia,

        -- Conteo de mediciones del día
        count(*)                                       as total_mediciones,

        -- Nivel del agua (m)
        round(avg(nivel_m), 3)                         as nivel_promedio,
        round(min(nivel_m), 3)                         as nivel_minimo,
        round(max(nivel_m), 3)                         as nivel_maximo,
        round(max(nivel_m) - min(nivel_m), 3)          as nivel_variacion,

        -- Conductividad eléctrica (µS/cm)
        round(avg(conductividad_us_cm), 3)             as conductividad_promedio,
        round(min(conductividad_us_cm), 3)             as conductividad_minima,
        round(max(conductividad_us_cm), 3)             as conductividad_maxima,

        -- pH (adimensional)
        round(avg(ph), 2)                              as ph_promedio,
        round(min(ph), 2)                              as ph_minimo,
        round(max(ph), 2)                              as ph_maximo,

        -- Turbidez (NTU)
        round(avg(turbidez_ntu), 2)                    as turbidez_promedio,
        round(min(turbidez_ntu), 2)                    as turbidez_minima,
        round(max(turbidez_ntu), 2)                    as turbidez_maxima,

        -- Oxígeno disuelto (mg/L)
        round(avg(od_mg_l), 3)                         as od_promedio,
        round(min(od_mg_l), 3)                         as od_minimo,
        round(max(od_mg_l), 3)                         as od_maximo,

        -- Temperatura del agua (°C)
        round(avg(temp_agua_c), 2)                     as temperatura_promedio,
        round(min(temp_agua_c), 2)                     as temperatura_minima,
        round(max(temp_agua_c), 2)                     as temperatura_maxima

    from mediciones
    group by estacion, estacion_codigo, fecha_dia
)

select * from diario
