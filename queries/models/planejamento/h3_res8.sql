{{
    config(
        materialized="table",
    )
}}


select
    tile_id,
    st_geogfromtext(geometry) as geometry_hex,
    st_centroid(st_geogfromtext(geometry)) as geometry_centroid
from {{ source("br_rj_riodejaneiro_geo", "h3_res8") }}
