{{ config(materialized="ephemeral") }}

with
    ordem_servico as (
        select
            * except (horario_inicio, horario_fim),
            split(horario_inicio, ":") horario_inicio_parts,
            split(horario_fim, ":") horario_fim_parts
        {# from `rj-smtr.gtfs.ordem_servico` #}
        from {{ ref("ordem_servico_gtfs") }}
    )
select
    * except (horario_fim_parts, horario_inicio_parts),
    make_interval(
        hour => cast(horario_inicio_parts[0] as integer),
        minute => cast(horario_inicio_parts[1] as integer),
        second => cast(horario_inicio_parts[2] as integer)
    ) as horario_inicio,
    make_interval(
        hour => cast(horario_fim_parts[0] as integer),
        minute => cast(horario_fim_parts[1] as integer),
        second => cast(horario_fim_parts[2] as integer)
    ) as horario_fim
from ordem_servico
