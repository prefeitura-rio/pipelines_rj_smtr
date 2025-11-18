{{
    config(
        materialized="view",
    )
}}

select tile_id, resolution, parent_id, geometry as geometry_wkt
from {{ source("br_rj_riodejaneiro_geo", "h3_res8") }}
