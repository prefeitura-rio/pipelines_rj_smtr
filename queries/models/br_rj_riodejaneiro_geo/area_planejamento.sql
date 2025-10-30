{{
    config(
        materialized="view",
    )
}}

select 
  area_planejamento ,
  ST_UNION_AGG(ST_GEOGFROMTEXT(geometry)) geometria
from `rj-smtr.br_rj_riodejaneiro_geo.bairros`
group by area_planejamento