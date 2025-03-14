-- depends_on: {{ ref('subsidio_data_versao_efetiva') }}
{{
    config(
        materialized="ephemeral",
    )
}}

{# ~ ref("subsidio_data_versao_efetiva") #}
{% if execute %}
    {% set result = run_query(
        "SELECT feed_start_date FROM `rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva`"
        ~ " WHERE data BETWEEN DATE_SUB(DATE('"
        ~ var("run_date")
        ~ "'), INTERVAL 2 DAY) AND DATE_SUB(DATE('"
        ~ var("run_date")
        ~ "'), INTERVAL 1 DAY)"
    ) %}
    {% set feed_start_dates = result.columns[0].values() %}
{% endif %}

{% if var("run_date") == "2024-05-05" %}
    -- Apuração "Madonna · The Celebration Tour in Rio"
    {% set gps_interval = 7 %}
{% else %} {% set gps_interval = 3 %}
{% endif %}


-- 1. Seleciona sinais de GPS registrados no período
with
    gps as (
        select
            g.* except (longitude, latitude, servico),
            servico,
            substr(id_veiculo, 2, 3) as id_empresa,
            st_geogpoint(longitude, latitude) posicao_veiculo_geo,
            date_sub(date("{{ var('run_date') }}"), interval 1 day) as data_operacao
        from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` g
        {# {{ ref("gps_sppo") }} g #}
        where
            (
                data between date_sub(
                    date("{{ var('run_date') }}"), interval 1 day
                ) and date("{{ var('run_date') }}")
            )
            -- Limita range de busca do gps de D-2 às 00h até D-1 às 3h
            and (
                timestamp_gps
                between datetime_sub(
                    datetime_trunc("{{ var('run_date') }}", day),
                    interval 1 day
                ) and datetime_add(
                    datetime_trunc("{{ var('run_date') }}", day),
                    interval {{ gps_interval }} hour
                )
            )
            and status != "Parado garagem"
    ),
    -- 2. Busca os shapes em formato geográfico
    shapes as (
        select *
        from
            {# {{ ref("shapes_geom_gtfs") }} #}
            `rj-smtr.gtfs.shapes_geom`
        where feed_start_date in ('{{ feed_start_dates|join("', '") }}')
    ),
    -- 3. Deduplica viagens planejadas
    viagem_planejada as (
        select distinct
            * except (
                faixa_horaria_inicio,
                faixa_horaria_fim,
                partidas_total_planejada,
                distancia_total_planejada,
                shape,
                start_pt,
                end_pt
            )
        from
            {# {{ ref("viagem_planejada") }} #}
            `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
        where data = date_sub(date("{{ var('run_date') }}"), interval 1 day)
    ),
    deduplica_viagem_planejada as (
        select v.*, s.shape, s.start_pt, s.end_pt
        from viagem_planejada as v
        left join shapes as s using (feed_version, feed_start_date, shape_id)
    ),
    -- 4. Classifica a posição do veículo em todos os shapes possíveis de
    -- serviços de uma mesma empresa
    status_viagem as (
        select
            data_operacao as data,
            g.id_veiculo,
            g.id_empresa,
            g.timestamp_gps,
            timestamp_trunc(g.timestamp_gps, minute) as timestamp_minuto_gps,
            g.posicao_veiculo_geo,
            trim(g.servico, " ") as servico_informado,
            s.servico as servico_realizado,
            s.shape_id,
            s.sentido_shape,
            s.shape_id_planejado,
            s.trip_id,
            s.trip_id_planejado,
            s.sentido,
            s.start_pt,
            s.end_pt,
            s.distancia_planejada,
            ifnull(g.distancia, 0) as distancia,
            s.feed_start_date,
            case
                when st_dwithin(g.posicao_veiculo_geo, start_pt, {{ var("buffer") }})
                then 'start'
                when st_dwithin(g.posicao_veiculo_geo, end_pt, {{ var("buffer") }})
                then 'end'
                when st_dwithin(g.posicao_veiculo_geo, shape, {{ var("buffer") }})
                then 'middle'
                else 'out'
            end status_viagem
        from gps g
        inner join
            (select * from deduplica_viagem_planejada) s
            on g.data_operacao = s.data
            and g.servico = s.servico
    ),
    buffer_primeiro_segmento as (
        select shape_id, feed_start_date, buffer as buffer_inicio
        {# from {{ ref("segmento_shape") }} #}
        from `rj-smtr.planejamento.segmento_shape`
        where
            feed_start_date in ('{{ feed_start_dates|join("', '") }}')
            and id_segmento = '1'
    ),
    ultimos_segmentos as (
        select
            shape_id,
            feed_start_date,
            max(safe_cast(id_segmento as int64)) as max_id_segmento
        {# from {{ ref("segmento_shape") }} #}
        from `rj-smtr.planejamento.segmento_shape`
        where feed_start_date in ('{{ feed_start_dates|join("', '") }}')
        group by shape_id, feed_start_date
    ),
    buffer_ultimo_segmento as (
        select l.shape_id, l.feed_start_date, s.buffer as buffer_fim
        from ultimos_segmentos l
        join
            {# {{ ref("segmento_shape") }} s #}
            `rj-smtr.planejamento.segmento_shape` s
            on l.shape_id = s.shape_id
            and l.feed_start_date = s.feed_start_date
            and l.max_id_segmento = safe_cast(s.id_segmento as int64)
    ),
    segmentos as (
        select f.shape_id, f.feed_start_date, f.buffer_inicio, l.buffer_fim
        from buffer_primeiro_segmento f
        join
            buffer_ultimo_segmento l
            on f.shape_id = l.shape_id
            and f.feed_start_date = l.feed_start_date
    ),
    -- Adiciona o indicador de interseção com o primeiro e último segmento, meio sempre true
    status_viagem_com_indicadores as (
        select
            sv.*,
            case
                when
                    sv.status_viagem = 'start'
                    and st_intersects(sv.posicao_veiculo_geo, se.buffer_inicio)
                then true
                when
                    sv.status_viagem = 'end'
                    and st_intersects(sv.posicao_veiculo_geo, se.buffer_fim)
                then true
                when sv.status_viagem = 'middle'
                then true
                else false
            end as indicador_intersecao_segmento,
        from status_viagem sv
        left join
            segmentos se
            on sv.shape_id = se.shape_id
            and sv.feed_start_date = se.feed_start_date
    )
select *, '{{ var("version") }}' as versao_modelo
from status_viagem_com_indicadores
