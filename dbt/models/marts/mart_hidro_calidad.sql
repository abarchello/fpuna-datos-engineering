{{ config(materialized='table') }}

/*
  Modelo mart: mart_hidro_calidad
  -------------------------------
  Monitoreo de calidad de datos hidrometricos por dia y estacion.
  Cuantifica cuantos valores fueron descartados como outliers durante
  la limpieza en stg_hidro_mediciones.

  Esta tabla permite:
    - Auditar la calidad del sensor por estacion y dia
    - Detectar periodos de descalibracion
    - Reportar en el dashboard la confiabilidad de los datos

  Capa: marts (consumo final para BI y reportes de calidad)
*/

with mediciones as (
    select * from {{ ref('stg_hidro_mediciones') }}
)

select
    estacion,
    estacion_codigo,
    fecha_dia,

    -- Volumen total de mediciones recibidas (esperado ~72/dia con cadencia 20 min)
    count(*)                                                                as total_mediciones,

    -- Cantidad de outliers descartados por cada metrica
    sum(is_outlier_nivel)                                                   as outliers_nivel,
    sum(is_outlier_conductividad)                                           as outliers_conductividad,
    sum(is_outlier_ph)                                                      as outliers_ph,
    sum(is_outlier_turbidez)                                                as outliers_turbidez,
    sum(is_outlier_od)                                                      as outliers_od,
    sum(is_outlier_temp)                                                    as outliers_temperatura,

    -- Total de outliers del dia (suma de todas las metricas)
    sum(is_outlier_nivel + is_outlier_conductividad + is_outlier_ph +
        is_outlier_turbidez + is_outlier_od + is_outlier_temp)              as outliers_totales,

    -- Tasa porcentual de outliers (sobre 6 metricas x total_mediciones posibles)
    round(
        100.0 * sum(is_outlier_nivel + is_outlier_conductividad + is_outlier_ph +
                    is_outlier_turbidez + is_outlier_od + is_outlier_temp)
        / nullif(count(*) * 6, 0)
    , 2)                                                                    as tasa_outliers_pct,

    -- Mediciones validas por metrica (despues de la limpieza)
    count(nivel_m)              as mediciones_validas_nivel,
    count(conductividad_us_cm)  as mediciones_validas_conductividad,
    count(ph)                   as mediciones_validas_ph,
    count(turbidez_ntu)         as mediciones_validas_turbidez,
    count(od_mg_l)              as mediciones_validas_od,
    count(temp_agua_c)          as mediciones_validas_temperatura,

    -- Clasificacion de calidad del dia
    case
        when round(
            100.0 * sum(is_outlier_nivel + is_outlier_conductividad + is_outlier_ph +
                        is_outlier_turbidez + is_outlier_od + is_outlier_temp)
            / nullif(count(*) * 6, 0)
        , 2) = 0   then 'optima'
        when round(
            100.0 * sum(is_outlier_nivel + is_outlier_conductividad + is_outlier_ph +
                        is_outlier_turbidez + is_outlier_od + is_outlier_temp)
            / nullif(count(*) * 6, 0)
        , 2) < 5   then 'buena'
        when round(
            100.0 * sum(is_outlier_nivel + is_outlier_conductividad + is_outlier_ph +
                        is_outlier_turbidez + is_outlier_od + is_outlier_temp)
            / nullif(count(*) * 6, 0)
        , 2) < 20  then 'regular'
        else            'mala'
    end                                                                     as nivel_calidad

from mediciones
group by estacion, estacion_codigo, fecha_dia
order by estacion, fecha_dia
