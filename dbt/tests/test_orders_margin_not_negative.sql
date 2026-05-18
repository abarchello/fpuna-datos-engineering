-- Singular test: verifica que ningun pedido tenga margen negativo excesivo
-- Se toleran margenes hasta -5 USD por descuentos, pero no mas

select
    order_id,
    price_usd,
    cogs_usd,
    margin_usd
from {{ ref('stg_orders') }}
where margin_usd < -5
