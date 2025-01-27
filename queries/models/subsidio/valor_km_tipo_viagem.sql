{{
    config(
        materialized="table",
    )
}}

select
    safe_cast(status as string) as status,
    nullif(safe_cast(tecnologia as string), '') as tecnologia,
    safe_cast(subsidio_km as float64) as subsidio_km,
    safe_cast(irk as float64) as irk,
    safe_cast(data_inicio as date) as data_inicio,
    safe_cast(data_fim as date) as data_fim,
    safe_cast(indicador_penalidade_judicial as bool) as indicador_penalidade_judicial,
    safe_cast(legislacao as string) as legislacao
from {{ source("subsidio_staging", "valor_km_tipo_viagem") }}
