{{
    config(
        materialized="view",
    )
}}

select *
from {{ source("br_rj_riodejaneiro_brt_gps","registro_historico") }}
