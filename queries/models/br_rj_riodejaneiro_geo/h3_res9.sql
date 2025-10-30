{{
    config(
        materialized="view",
    )
}}

select *
from {{ source("br_rj_riodejaneiro_geo", "h3_res9") }}
