{{
    config(
        materialized="table",
    )
}}

select perc_km_inferior, perc_km_superior, tipo_penalidade, valor, data_inicio, data_fim
from {{ ref("staging_valor_tipo_penalidade") }}
