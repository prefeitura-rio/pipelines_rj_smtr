{{
    config(
        materialized="table",
    )
}}

select
    inicio_vigencia,
    fim_vigencia,
    operador,
    endereco,
    bairro,
    indicador_ativa,
    tipo_uso,
    area,
    geometry_wkt,
    st_geogfromtext(geometry_wkt, make_valid => true) as geometry
from {{ ref("staging_garagem") }}
