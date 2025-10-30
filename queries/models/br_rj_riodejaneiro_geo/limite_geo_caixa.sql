{{
    config(
        materialized="view",
    )
}}

select *
from {{ source("br_rj_riodejaneiro_geo", "limite_geo_caixa") }}
