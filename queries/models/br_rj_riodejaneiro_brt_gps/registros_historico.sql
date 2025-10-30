{{
    config(
        materialized="view",
    )
}}

select *
from {{ source("br_rj_riodejaneiro_brt_gps", "registros_historico") }}
