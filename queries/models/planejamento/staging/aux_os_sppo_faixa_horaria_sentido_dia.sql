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
        {# from {{ ref("aux_ordem_servico_faixa_horaria") }} #}
        {# from `rj-smtr.planejamento_staging.aux_ordem_servico_faixa_horaria` #}
        from {{ ref("ordem_servico_faixa_horaria") }}
    {# from `rj-smtr.planejamento.ordem_servico_faixa_horaria` #}
    ),
    os_tratamento_horario as (
        select
            *,
            make_interval(
                hour => cast(faixa_horaria_inicio_parts[0] as integer),
                minute => cast(faixa_horaria_inicio_parts[1] as integer),
                second => cast(faixa_horaria_inicio_parts[2] as integer)
            ) as faixa_horario_intervalo_inicio,
            make_interval(
                hour => cast(faixa_horaria_fim_parts[0] as integer),
                minute => cast(faixa_horaria_fim_parts[1] as integer),
                second => cast(faixa_horaria_fim_parts[2] as integer)
            ) as faixa_horario_intervalo_fim
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
            c.data + o.faixa_horario_intervalo_inicio as faixa_horaria_inicio,
            c.data + o.faixa_horario_intervalo_fim as faixa_horaria_fim,
            partidas_ida,
            partidas_volta,
            quilometragem,
            partidas
        from {{ ref("calendario") }} c
        {# from `rj-smtr.planejamento.calendario` c #}
        join
            os_tratamento_horario o using (
                feed_version, feed_start_date, tipo_dia, tipo_os
            )
    ),
    faixas_agregadas as (
        select
            * except (partidas_ida, partidas_volta, partidas, quilometragem),
            case
                when
                    lag(faixa_horaria_inicio) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    = faixa_horaria_inicio
                then
                    lag(partidas_ida) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    + partidas_ida
                else partidas_ida
            end as partidas_ida,
            case
                when
                    lag(faixa_horaria_inicio) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    = faixa_horaria_inicio
                then
                    lag(partidas_volta) over (
                        partition by servico order by data, faixa_horaria_inicio
                    )
                    + partidas_volta
                else partidas_volta
            end as partidas_volta,
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
    ),
    os_filtrada as (
        select *
        from faixas_agregadas
        where rn = 1 and data = extract(date from faixa_horaria_inicio)
    ),
    os_por_sentido as (
        select
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            sentido.codigo as sentido,
            sentido.extensao as extensao,
            sentido.partidas as partidas,
            quilometragem,  -- TODO: alterar para sentido.extensao * sentido.partidas / 1000 quando subir novo modelo de OS
            faixa_horaria_inicio,
            faixa_horaria_fim
        from
            os_filtrada,
            unnest(
                [
                    struct(
                        'I' as codigo,
                        extensao_ida as extensao,
                        partidas_ida as partidas
                    ),
                    struct(
                        'V' as codigo,
                        extensao_volta as extensao,
                        partidas_volta as partidas
                    )
                ]
            ) as sentido
    )
select *
from os_por_sentido
