{{
    config(
        materialized='table',
        tags=['geolocalizacao']
    )
}}

SELECT
    g.id_servico AS id_servico_gtfs,
    j.cd_linha AS id_servico_jae,
    COALESCE(g.servico, j.nr_linha) AS servico,
    g.servico AS servico_gtfs,
    j.nr_linha AS servico_jae,
    COALESCE(g.descricao_servico, j.nm_linha) AS descricao_servico,
    g.descricao_servico AS descricao_servico_gtfs,
    j.nm_linha AS descricao_servico_jae,
    g.latitude,
    g.longitude,
    g.tabela_origem_gtfs,
    COALESCE(g.inicio_vigencia, DATE(j.datetime_inclusao)) AS data_inicio_vigencia,
    g.fim_vigencia AS data_fim_vigencia,
    '{{ var("version") }}' as versao
FROM
    {{ ref("staging_linha") }} j
FULL OUTER JOIN
    {{ ref("aux_servicos_gtfs") }} g
ON
    COALESCE(j.gtfs_route_id, j.gtfs_stop_id) = g.id_servico