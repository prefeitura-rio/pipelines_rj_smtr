{{
    config(
        materialized="view",
    )
}}

select *
from {{ source("br_rj_riodejaneiro_geo", "garagens_polygon") }}
