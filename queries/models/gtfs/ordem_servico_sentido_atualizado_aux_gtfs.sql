/*
  ordem_servico_gtfs com sentidos despivotados, ajustes nos horários e com atualização dos sentidos circulares
*/
{{ config(materialized="ephemeral") }}

with
    -- 1. Identifica o sentido de cada serviço
    servico_trips_sentido as (
        select distinct *
        from
            (
                select
                    feed_version,
                    trip_short_name as servico,
                    case
                        when
                            round(st_y(start_pt), 4) = round(st_y(end_pt), 4)
                            and round(st_x(start_pt), 4) = round(st_x(end_pt), 4)
                        then "C"
                        when direction_id = "0"
                        then "I"
                        when direction_id = "1"
                        then "V"
                    end as sentido
                from {{ ref("trips_filtrada_aux_gtfs") }}
                where indicador_trajeto_alternativo is false
            )
        where sentido = "C"
    ),
    -- 2. Busca principais informações na Ordem de Serviço (OS)
    ordem_servico as (
        select
            * except (horario_inicio, horario_fim),
            horario_inicio as inicio_periodo,
            horario_fim as fim_periodo,
        from {{ ref("aux_ordem_servico_diaria") }}
        {% if is_incremental() -%}
            where feed_start_date = '{{ var("data_versao_gtfs") }}'
        {%- endif %}
    ),
    -- 3. Despivota ordem de serviço por sentido
    ordem_servico_sentido as (
        select *
        from
            ordem_servico unpivot (
                (distancia_planejada, partidas) for sentido in (
                    (extensao_ida, partidas_ida) as "I",
                    (extensao_volta, partidas_volta) as "V"
                )
            )
    )
-- 4. Atualiza sentido dos serviços circulares na ordem de serviço
select o.* except (sentido), coalesce(s.sentido, o.sentido) as sentido
from ordem_servico_sentido as o
left join servico_trips_sentido as s using (feed_version, servico)
where
    distancia_planejada != 0
    and (
        (
            feed_start_date < '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
            and (distancia_total_planejada != 0 and (partidas != 0 or partidas is null))
        )
        or feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
    )
