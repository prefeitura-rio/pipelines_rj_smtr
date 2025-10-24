{{
    config(
        materialized="view",
    )
}}

select
    m.*,
    ho.geometry_centroid as origem_centroide,
    hd.geometry_centroid as destino_centroide,
from {{ ref("matriz_origem_destino_bruta") }} as m
left join {{ ref("h3_res8") }} as ho on ho.tile_id = tile_id_origem
left join {{ ref("h3_res8") }} as hd on hd.tile_id = tile_id_destino
