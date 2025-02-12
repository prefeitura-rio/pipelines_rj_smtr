{{ config(materialized="ephemeral") }}

with
    ordem_servico as (
        select
            * except (horario_inicio, horario_fim),
            parse_time("%H:%M:%S", lpad(horario_inicio, 8, '0')) as horario_inicio,
            split(horario_fim, ":") horario_fim_parts
        from {{ ref("aux_ordem_servico_diaria") }}
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
