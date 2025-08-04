{{ config(materialized="table", tags=["geolocalizacao"]) }}

with
    linha_jae as (
        select *
        from {{ ref("staging_linha") }}
        qualify
            row_number() over (partition by cd_linha order by timestamp_captura desc)
            = 1
    )
select
    g.id_servico as id_servico_gtfs,
    j.cd_linha as id_servico_jae,
    coalesce(g.servico, j.nr_linha) as servico,
    g.servico as servico_gtfs,
    j.nr_linha as servico_jae,
    coalesce(g.descricao_servico, j.nm_linha) as descricao_servico,
    g.descricao_servico as descricao_servico_gtfs,
    j.nm_linha as descricao_servico_jae,
    g.latitude,
    g.longitude,
    g.tabela_origem_gtfs,
    coalesce(g.inicio_vigencia, date(j.datetime_inclusao)) as data_inicio_vigencia,
    g.fim_vigencia as data_fim_vigencia,
    '{{ var("version") }}' as versao
from linha_jae j
full outer join
    {{ ref("aux_servicos_gtfs") }} g
    on coalesce(j.gtfs_route_id, j.gtfs_stop_id) = g.id_servico
