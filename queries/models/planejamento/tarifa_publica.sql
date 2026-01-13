{{
    config(
        materialized="table",
    )
}}

select
    date(a.data_inicio) as data_inicio,
    date_sub(
        date(lead(a.data_inicio) over (order by a.data_inicio)), interval 1 day
    ) as data_fim,
    a.valor_tarifa
from
    unnest(
        [
            struct("2023-01-07" as data_inicio, 4.3 as valor_tarifa),
            struct("2025-01-05" as data_inicio, 4.7 as valor_tarifa),
            struct("2026-01-04" as data_inicio, 5.0 as valor_tarifa)
        ]
    ) as a
