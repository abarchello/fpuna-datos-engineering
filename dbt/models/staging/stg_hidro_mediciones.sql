{{ config(materialized='view') }}

/*
  Modelo staging: stg_hidro_mediciones
  ------------------------------------
  Unifica las mediciones de las estaciones automaticas (Itabo Guazu y
  Pozuelo Puente) en una sola tabla con esquema normalizado. Parsea el
  JSON crudo y aplica LIMPIEZA ACTIVA de outliers fisicamente
  implausibles (sensores descalibrados o fuera de servicio).

  Reglas de limpieza aplicadas (valores fuera de rango -> NULL):
    nivel_m              : valido en [0, 30] m
    conductividad_us_cm  : valido en [0, 2000] uS/cm
    ph                   : valido en [4, 10]   (rango realista agua natural)
    turbidez_ntu         : valido en [0, 3000] NTU
    od_mg_l              : valido en [0, 15]   (saturacion teorica ~14)
    temp_agua_c          : valido en [5, 40]   grados C

  Para cada metrica se expone tambien un flag is_outlier_* que vale 1
  cuando el dato crudo existia pero fue descartado. Esto permite
  cuantificar la calidad en el mart mart_hidro_calidad.

  Origen:  API REST Institucional Hidrometeorologica (estaciones automaticas)
  Capa:    staging (limpieza y tipado)
  Cadencia: ~1 medicion cada 20 minutos por estacion
*/

with itabo_guazu as (
    select station_name, measurement_ts, raw_payload, extracted_at
    from {{ source('hidro', 'hidro_estacion_itabo_guazu') }}
),

pozuelo_puente as (
    select station_name, measurement_ts, raw_payload, extracted_at
    from {{ source('hidro', 'hidro_estacion_pozuelo_puente') }}
),

unioned as (
    select * from itabo_guazu
    union all
    select * from pozuelo_puente
),

parsed as (
    select
        case station_name
            when 'itabo_guazu'    then 'Itabó Guazú'
            when 'pozuelo_puente' then 'Pozuelo Puente'
            else station_name
        end                                                                as estacion,
        station_name                                                       as estacion_codigo,

        try_cast(substr(measurement_ts, 1, 19) as timestamp)               as fecha_medicion,
        date_trunc('day',   try_cast(substr(measurement_ts, 1, 19) as timestamp))  as fecha_dia,
        date_trunc('hour',  try_cast(substr(measurement_ts, 1, 19) as timestamp))  as fecha_hora,
        date_trunc('month', try_cast(substr(measurement_ts, 1, 19) as timestamp))  as fecha_mes,

        -- Lecturas crudas (preservadas para auditoria)
        try_cast(json_extract_string(raw_payload, '$.nivel')         as double)  as nivel_raw,
        try_cast(json_extract_string(raw_payload, '$.conductividad') as double)  as conductividad_raw,
        try_cast(json_extract_string(raw_payload, '$.ph')            as double)  as ph_raw,
        try_cast(json_extract_string(raw_payload, '$.turbidez')      as double)  as turbidez_raw,
        try_cast(json_extract_string(raw_payload, '$.od')            as double)  as od_raw,
        try_cast(json_extract_string(raw_payload, '$.tempagua')      as double)  as temp_raw,

        extracted_at                                                       as cargado_en

    from unioned
    where measurement_ts is not null
),

cleaned as (
    select
        estacion,
        estacion_codigo,
        fecha_medicion,
        fecha_dia,
        fecha_hora,
        fecha_mes,

        -- Lecturas crudas preservadas
        nivel_raw,
        conductividad_raw,
        ph_raw,
        turbidez_raw,
        od_raw,
        temp_raw,

        -- Lecturas LIMPIAS (NULL si estan fuera de rango fisico plausible)
        case when nivel_raw         between 0 and 30   then nivel_raw         end as nivel_m,
        case when conductividad_raw between 0 and 2000 then conductividad_raw end as conductividad_us_cm,
        case when ph_raw            between 4 and 10   then ph_raw            end as ph,
        case when turbidez_raw      between 0 and 3000 then turbidez_raw      end as turbidez_ntu,
        case when od_raw            between 0 and 15   then od_raw            end as od_mg_l,
        case when temp_raw          between 5 and 40   then temp_raw          end as temp_agua_c,

        -- Flags de outliers (1 = el dato crudo existia pero se descarto por estar fuera de rango)
        case when nivel_raw         is not null and nivel_raw         not between 0 and 30   then 1 else 0 end as is_outlier_nivel,
        case when conductividad_raw is not null and conductividad_raw not between 0 and 2000 then 1 else 0 end as is_outlier_conductividad,
        case when ph_raw            is not null and ph_raw            not between 4 and 10   then 1 else 0 end as is_outlier_ph,
        case when turbidez_raw      is not null and turbidez_raw      not between 0 and 3000 then 1 else 0 end as is_outlier_turbidez,
        case when od_raw            is not null and od_raw            not between 0 and 15   then 1 else 0 end as is_outlier_od,
        case when temp_raw          is not null and temp_raw          not between 5 and 40   then 1 else 0 end as is_outlier_temp,

        cargado_en

    from parsed
    where fecha_medicion is not null
)

select * from cleaned
