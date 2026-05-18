-- Singular test: verifica que ninguna medicion hidrometrica tenga
-- timestamp futuro. Un sensor que reporta fechas futuras indica
-- error de reloj interno o problema de zona horaria.

select
    estacion,
    fecha_medicion,
    nivel_raw,
    cargado_en
from {{ ref('stg_hidro_mediciones') }}
where fecha_medicion > current_timestamp
