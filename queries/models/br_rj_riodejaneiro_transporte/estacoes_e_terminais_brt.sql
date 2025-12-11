{{
    config(
        materialized="table",
    )
}}

select *
from {{ source("br_rj_riodejaneiro_transporte_staging", "estacoes_e_terminais_brt") }}
