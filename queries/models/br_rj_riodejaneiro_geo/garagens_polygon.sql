{{
    config(
        materialized="view",
        alias = "garagem_polygon"
    )
}}

select WKT, NM_MUNICIP, CD_GEOCMU
from {{ source("br_rj_riodejaneiro_geo", "garagens_polygon") }}
