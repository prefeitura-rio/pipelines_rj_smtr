{{
    config(
        materialized="ephemeral",
    )
}}

with
    os as (select * from {{ ref("aux_os_sppo_faixa_horaria_dia") }}),
    sentido as (
        select feed_start_date, tipo_os, tipo_dia, servico, sentido, distancia_planejada
        from {{ ref("ordem_servico_sentido_atualizado_aux_gtfs") }}
    ),
    os_sentido as (
        select
            o.data,
            o.feed_version,
            o.feed_start_date,
            o.feed_end_date,
            o.tipo_os,
            o.tipo_dia,
            o.servico,
            s.sentido,
            o.vista,
            o.consorcio,
            o.horario_inicio,
            o.horario_fim,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
            s.distancia_planejada as extensao,
            case
                when s.sentido in ("I", "C") then partidas_ida else partidas_volta
            end as partidas,
            case
                when s.sentido in ("I", "C")
                then partidas_ida * distancia_planejada
                else partidas_volta * distancia_planejada
            end as quilometragem,
        from os o
        left join sentido s using (feed_start_date, tipo_os, tipo_dia, servico)
    )
select *
from os_sentido
