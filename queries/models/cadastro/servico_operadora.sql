{{ config(materialized="table") }}

with
    linha_tarifa as (
        select
            cd_linha,
            vl_tarifa_ida as tarifa_ida,
            vl_tarifa_volta as tarifa_volta,
            dt_inicio_validade,
            lead(dt_inicio_validade) over (
                partition by cd_linha order by nr_sequencia
            ) as data_fim_validade
        from {{ ref("staging_linha_tarifa") }}
    ),
    tratado as (
        select
            c.modo,
            c.id_consorcio,
            c.consorcio,
            o.id_operadora,
            o.operadora,
            lco.cd_linha as id_servico_jae,
            l.nr_linha as servico_jae,
            l.nm_linha as descricao_servico_jae,
            coalesce(l.gtfs_route_id, l.gtfs_stop_id) as id_servico_gtfs,
            case
                when l.gtfs_route_id is not null
                then 'routes'
                when l.gtfs_stop_id is not null
                then 'stops'
            end as tabela_origem_gtfs,
            lt.tarifa_ida,
            lt.tarifa_volta,
            case
                when datetime(lco.dt_inicio_validade) > lt.dt_inicio_validade
                then datetime(lco.dt_inicio_validade)
                else lt.dt_inicio_validade
            end as data_inicio_validade,
            case
                when lco.dt_fim_validade is null and lt.data_fim_validade is not null
                then lt.data_fim_validade
                when lco.dt_fim_validade is not null and lt.data_fim_validade is null
                then lco.dt_fim_validade
                when datetime(lco.dt_fim_validade) > lt.data_fim_validade
                then lt.data_fim_validade
                when datetime(lco.dt_fim_validade) < lt.data_fim_validade
                then datetime(lco.dt_fim_validade)
            end as data_fim_validade
        from {{ ref("staging_linha_consorcio_operadora_transporte") }} lco
        join
            {{ ref("operadoras") }} o
            on lco.cd_operadora_transporte = o.id_operadora_jae
        join {{ ref("consorcios") }} c on lco.cd_consorcio = c.id_consorcio_jae
        join {{ ref("staging_linha") }} l on lco.cd_linha = l.cd_linha
        left join linha_tarifa lt on lco.cd_linha = lt.cd_linha
        where
            (
                (
                    lt.data_fim_validade is not null
                    and datetime(lco.dt_inicio_validade) < lt.data_fim_validade
                )
                and (
                    lco.dt_fim_validade is not null
                    and datetime(lt.dt_inicio_validade) < lco.dt_fim_validade
                )
            )
            or (lt.data_fim_validade is null or lco.dt_fim_validade is null)
    )
select *, '{{ var("version") }}' as versao
from tratado
where data_inicio_validade < data_fim_validade or data_fim_validade is null
