{{ config(materialized="view", enabled=false) }}

select area_planejamento, st_union_agg(st_geogfromtext(geometry)) geometria
from {{ source("br_rj_riodejaneiro_geo", "bairros") }}
group by area_planejamento
