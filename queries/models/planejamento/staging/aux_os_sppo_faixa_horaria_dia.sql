{{
    config(
        materialized="ephemeral",
    )
}}

with
    os as (
        select
            *,
            split(faixa_horaria_inicio, ":") as faixa_horaria_inicio_parts,
            split(faixa_horaria_fim, ":") as faixa_horaria_fim_parts
        {# from {{ ref("ordem_servico_faixa_horaria") }} #}
        from `rj-smtr.planejamento.ordem_servico_faixa_horaria`
    ),
    os_tratamento_horario as (
        select
            *,
            div(
                cast(faixa_horaria_inicio_parts[0] as integer), 24
            ) dia_soma_faixa_inicio,
            div(cast(faixa_horaria_fim_parts[0] as integer), 24) dia_soma_faixa_fim,
            concat(
                lpad(
                    cast(
                        if(
                            cast(faixa_horaria_inicio_parts[0] as integer) >= 24,
                            cast(faixa_horaria_inicio_parts[0] as integer) - 24,
                            cast(faixa_horaria_inicio_parts[0] as integer)
                        ) as string
                    ),
                    2,
                    '0'
                ),
                ":",
                faixa_horaria_inicio_parts[1],
                ":",
                faixa_horaria_inicio_parts[2]
            ) as faixa_horaria_tratada_inicio,
            concat(
                lpad(
                    cast(
                        if(
                            cast(faixa_horaria_fim_parts[0] as integer) >= 24,
                            cast(faixa_horaria_fim_parts[0] as integer) - 24,
                            cast(faixa_horaria_fim_parts[0] as integer)
                        ) as string
                    ),
                    2,
                    '0'
                ),
                ":",
                faixa_horaria_fim_parts[1],
                ":",
                faixa_horaria_fim_parts[2]
            ) as faixa_horaria_tratada_fim
        from os
    ),
    os_faixa_horaria_dia as (
        select
            c.data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            extensao_ida,
            extensao_volta,
            viagens_dia,
            datetime(
                concat(
                    cast(
                        date_add(c.data, interval o.dia_soma_faixa_inicio day) as string
                    ),
                    ' ',
                    o.faixa_horaria_tratada_inicio
                )
            ) as faixa_horaria_inicio,
            datetime(
                concat(
                    cast(date_add(c.data, interval o.dia_soma_faixa_fim day) as string),
                    ' ',
                    o.faixa_horaria_tratada_fim
                )
            ) as faixa_horaria_fim,
            o.quilometragem,
            o.partidas
        {# from {{ ref("calendario") }} c #}
        from `rj-smtr.planejamento.calendario` c
        join
            os_tratamento_horario o using (
                feed_version, feed_start_date, tipo_dia, tipo_os
            )
    ),
    faixas_agregadas as (
        select
            * except (partidas, quilometragem),
            case
                when
                    lag(faixa_horaria_inicio) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    = faixa_horaria_inicio
                then
                    lag(partidas) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    + partidas
                else partidas
            end as partidas,
            case
                when
                    lag(faixa_horaria_inicio) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    = faixa_horaria_inicio
                then
                    lag(quilometragem) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    + quilometragem
                else quilometragem
            end as quilometragem,
            row_number() over (
                partition by servico, faixa_horaria_inicio order by data desc
            ) as rn
        from os_faixa_horaria_dia
    )
select *
from faixas_agregadas
where rn = 1 and data = extract(date from faixa_horaria_inicio)
