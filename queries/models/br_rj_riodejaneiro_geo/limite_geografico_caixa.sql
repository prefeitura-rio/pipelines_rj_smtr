{{
    config(
        materialized="view",
    )
}}

select *
from {{ source("br_rj_riodejaneiro_geo", "limites_geograficos_caixa") }}
