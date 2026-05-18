{{ config(severity='warn') }}

-- Singular test: verifica que no haya nombres duplicados de pokemon.
-- Severity = warn porque Airbyte en modo Append puede sumar duplicados
-- cuando se sincroniza varias veces el mismo pokemon. Se documenta como
-- alerta en lugar de error para reflejar la realidad del sync.

select
    pokemon_name,
    count(*) as occurrences
from {{ ref('mart_pokemon_analytics') }}
group by pokemon_name
having count(*) > 1
