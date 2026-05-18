{{ config(materialized='table') }}

/*
  Modelo mart: mart_hidro_resumen
  -------------------------------
  One Big Table (OBT) con métricas analíticas hidrométricas por
  día/estación. Incluye clasificaciones del nivel del agua,
  alertas de calidad y deltas vs día anterior.

  Capa: marts (consumo final para BI/Metabase)
*/

with diario as (
    select * from {{ ref('int_hidro_diario') }}
),

con_deltas as (
    select
        d.*,

        -- Nivel del día anterior (LAG por estación)
        lag(nivel_promedio) over (
            partition by estacion order by fecha_dia
        ) as nivel_promedio_anterior,

        -- Promedio móvil 7 días para nivel
        round(avg(nivel_promedio) over (
            partition by estacion order by fecha_dia
            rows between 6 preceding and current row
        ), 3) as nivel_promedio_movil_7d,

        -- Promedio histórico de la estación (referencia global)
        round(avg(nivel_promedio) over (partition by estacion), 3) as nivel_historico_estacion,

        -- pH histórico y temperatura histórica
        round(avg(ph_promedio)           over (partition by estacion), 2) as ph_historico_estacion,
        round(avg(temperatura_promedio)  over (partition by estacion), 2) as temp_historica_estacion

    from diario d
),

enriched as (
    select
        estacion,
        estacion_codigo,
        fecha_dia,
        extract('year'  from fecha_dia)::int                as anio,
        extract('month' from fecha_dia)::int                as mes,
        extract('week'  from fecha_dia)::int                as semana,
        case extract('dow' from fecha_dia)::int
            when 0 then 'Domingo'
            when 1 then 'Lunes'
            when 2 then 'Martes'
            when 3 then 'Miércoles'
            when 4 then 'Jueves'
            when 5 then 'Viernes'
            when 6 then 'Sábado'
        end                                                  as dia_semana,

        total_mediciones,

        -- Nivel del agua
        nivel_promedio,
        nivel_minimo,
        nivel_maximo,
        nivel_variacion,
        nivel_promedio_movil_7d,
        nivel_historico_estacion,

        -- Calidad del agua
        conductividad_promedio,
        ph_promedio,
        ph_historico_estacion,
        turbidez_promedio,
        od_promedio,
        temperatura_promedio,
        temp_historica_estacion,

        -- Delta vs día anterior
        round(nivel_promedio - nivel_promedio_anterior, 3)        as nivel_delta_dia_anterior,

        -- Desviación vs promedio histórico
        round(nivel_promedio - nivel_historico_estacion, 3)       as nivel_desviacion_historica,

        -- Clasificación del nivel del agua
        case
            when nivel_promedio is null then 'sin_datos'
            when nivel_historico_estacion is null then 'sin_referencia'
            when nivel_promedio >= nivel_historico_estacion * 1.20 then 'crecida'
            when nivel_promedio >= nivel_historico_estacion * 1.05 then 'alto'
            when nivel_promedio <= nivel_historico_estacion * 0.80 then 'bajante'
            when nivel_promedio <= nivel_historico_estacion * 0.95 then 'bajo'
            else 'normal'
        end                                                       as condicion_nivel,

        -- Alerta de pH fuera del rango aceptable para agua natural (6.5 - 8.5)
        case
            when ph_promedio is null then 'sin_datos'
            when ph_promedio < 6.5  then 'acido'
            when ph_promedio > 8.5  then 'alcalino'
            else 'normal'
        end                                                       as condicion_ph,

        -- Alerta de turbidez (umbral OMS para agua dulce: 5 NTU recreación, 50 NTU límite)
        case
            when turbidez_promedio is null then 'sin_datos'
            when turbidez_promedio >= 50 then 'muy_turbia'
            when turbidez_promedio >= 5  then 'turbia'
            else 'clara'
        end                                                       as condicion_turbidez,

        -- Alerta de oxígeno disuelto (umbral biológico: >= 5 mg/L saludable)
        case
            when od_promedio is null then 'sin_datos'
            when od_promedio < 3 then 'critico'
            when od_promedio < 5 then 'bajo'
            else 'saludable'
        end                                                       as condicion_od

    from con_deltas
)

select * from enriched
order by estacion, fecha_dia
