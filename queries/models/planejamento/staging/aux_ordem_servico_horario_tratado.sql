{{ config(materialized="ephemeral") }}

with
    ordem_servico as (
        {% if var("data_versao_gtfs") < var("GTFS_DATA_MODELO_OS") %}
            select
                * except (horario_inicio, horario_fim),
                parse_time("%H:%M:%S", lpad(horario_inicio, 8, '0')) as horario_inicio,
                split(horario_fim, ":") horario_fim_parts
            {# from `rj-smtr.gtfs.ordem_servico` #}
            from {{ ref("ordem_servico_gtfs") }}
        {% else %}
            select
                feed_version,
                feed_start_date,
                feed_end_date,
                tipo_os,
                servico,
                vista,
                consorcio,
                extensao_ida,
                extensao_volta,
                tipo_dia,
                partidas_ida_dia as partidas_ida,
                partidas_volta_dia as partidas_volta,
                viagens_dia as viagens_planejadas,
                sum(quilometragem) as distancia_total_planejada,
                parse_time("%H:%M:%S", lpad(horario_inicio, 8, '0')) as horario_inicio,
                split(horario_fim, ":") horario_fim_parts,
            from {{ ref("ordem_servico_faixa_horaria") }}
            group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
        {% endif %}
    )
select
    * except (horario_fim_parts),
    div(cast(horario_fim_parts[0] as integer), 24) as dias_horario_fim,
    parse_time(
        "%H:%M:%S",
        concat(
            lpad(
                cast(
                    if(
                        cast(horario_fim_parts[0] as integer) >= 24,
                        cast(horario_fim_parts[0] as integer) - 24,
                        cast(horario_fim_parts[0] as integer)
                    ) as string
                ),
                2,
                '0'
            ),
            ":",
            horario_fim_parts[1],
            ":",
            horario_fim_parts[2]
        )
    ) as horario_fim,
from ordem_servico
