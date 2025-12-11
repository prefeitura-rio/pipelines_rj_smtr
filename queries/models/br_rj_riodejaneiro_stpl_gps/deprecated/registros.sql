{{
    config(
        materialized="view",
    )
}}

select *
from `rj-smtr.br_rj_riodejaneiro_stpl_gps.registros`
