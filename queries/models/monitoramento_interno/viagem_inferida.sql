{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["id_viagem"],
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

{# {% set calendario = ref("calendario") %} #}
{% set calendario = "rj-smtr.planejamento.calendario" %}
{% if execute %}
    {# {% if is_incremental() %} #}
    {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where {{ incremental_filter }}
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
{# {% endif %} #}
{% endif %}

with
    aux_status as (
        select
            * except (servico_viagem),
            servico_viagem as servico,
            case
                when status_viagem = 'end'
                then
                    last_value(
                        case when status_viagem = 'start' then timestamp_gps end
                    ) over (
                        partition by id_veiculo, shape_id
                        order by timestamp_gps
                        rows between unbounded preceding and 1 preceding
                    )
            end as datetime_partida
        from {{ ref("aux_monitoramento_registros_status_trajeto") }}
        where status_viagem in ('start', 'end')
    ),
    routes as (
        select *
        {# from {{ ref("routes_gtfs") }} #}
        from `rj-smtr.gtfs.routes`
        {# {% if is_incremental() %} #}
        where feed_start_date in ({{ gtfs_feeds | join(", ") }})
    {# {% endif %} #}
    ),
    viagens as (
        select distinct
            concat(
                id_veiculo,
                "-",
                servico,
                "-",
                sentido,
                "-",
                shape_id,
                "-",
                format_datetime("%Y%m%d%H%M%S", datetime_partida)
            ) as id_viagem,
            data,
            id_empresa,
            id_veiculo,
            servico_gps,
            servico,
            case
                {# when v.fonte_gps = 'brt'
        then 'BRT' #}
                when r.route_type = '200'
                then 'Ônibus Executivo'
                when r.route_type = '700'
                then 'Ônibus SPPO'
            end as modo,
            trip_id,
            route_id,
            shape_id,
            sentido,
            distancia_planejada,
            datetime_partida,
            timestamp_gps as datetime_chegada,
            '{{ var("version") }}' as versao_modelo
        from aux_status
        left join routes r using (route_id, feed_start_date)
        where
            status_viagem = 'end'
            and datetime_partida is not null
            and datetime_partida < timestamp_gps
        qualify
            row_number() over (
                partition by id_veiculo, shape_id, datetime_partida
                order by timestamp_gps
            )
            = 1
    ),
    viagens_sobrepostas as (
        select
            v1.data,
            v1.id_viagem,
            v1.id_veiculo,
            v1.datetime_partida,
            v1.datetime_chegada,
            case
                when v2.id_viagem is not null then true else false
            end as indicador_viagem_sobreposta
        from viagens v1
        left join
            viagens v2
            on (v1.data = v2.data or v1.data = date_add(v2.data, interval 1 day))
            and v1.id_veiculo = v2.id_veiculo
            and v1.id_viagem != v2.id_viagem
            and v1.datetime_partida < v2.datetime_chegada
            and v1.datetime_chegada > v2.datetime_partida
    )
select v.*, vs.indicador_viagem_sobreposta
from viagens v
left join viagens_sobrepostas vs on v.id_viagem = vs.id_viagem
