{{
    config(
        materialized="ephemeral",
    )
}}


with
    os_staging as (
        select
            "2024-11-16" as feed_version,
            date("2024-11-16") as feed_start_date,
            null as feed_end_date,
            "Regular" as tipo_os,
            `Serviço` as servico,
            `Consórcio` as consorcio,
            cast(
                replace(replace(`Horário Inicial`, ".", ""), ",", ".") as float64
            ) as extensao_ida,
            cast(
                replace(replace(`Horário Fim`, ".", ""), ",", ".") as float64
            ) as extensao_volta,
            cast(`Partidas Ida Dia Útil` as integer) as partidas_ida_du,
            cast(`Partidas Volta Dia Útil` as integer) as partidas_volta_du,
            cast(
                replace(replace(`Quilometragem Dia Útil`, ".", ""), ",", ".") as float64
            ) as km_du,
            cast(`Partidas Ida Sábado` as integer) as partidas_ida_sabado,
            cast(`Partidas Volta Sábado` as integer) as partidas_volta_sabado,
            cast(
                replace(replace(`Quilometragem Sábado`, ".", ""), ",", ".") as float64
            ) as km_sabado,
            cast(`Partidas Ida Domingo` as integer) as partidas_ida_domingo,
            cast(`Partidas Volta Domingo` as integer) as partidas_volta_domingo,
            cast(
                replace(replace(`Quilometragem Domingo`, ".", ""), ",", ".") as float64
            ) as km_domingo,
            cast(`Partidas Ida Ponto Facultativo` as integer) as partidas_ida_pf,
            cast(`Partidas Volta Ponto Facultativo` as integer) as partidas_volta_pf,
            cast(
                replace(
                    replace(`Quilometragem Ponto Facultativo`, ".", ""), ",", "."
                ) as float64
            ) as km_pf
        from `rj-smtr-dev.rafael__planejamento_staging.ordem_servico_brt`
    ),
    os_tipo_dia as (
        select *
        from
            os_staging unpivot (
                (partidas_ida, partidas_volta, quilometragem) for tipo_dia in (
                    (partidas_ida_du, partidas_volta_du, km_du) as 'Dia Útil',
                    (partidas_ida_pf, partidas_volta_pf, km_pf) as 'Ponto Facultativo',
                    (partidas_ida_sabado, partidas_volta_sabado, km_sabado) as 'Sabado',
                    (
                        partidas_ida_domingo, partidas_volta_domingo, km_domingo
                    ) as 'Domingo'
                )
            )
    ),
    os_sentido as (
        select *
        from
            os_tipo_dia unpivot (
                (extensao, partidas_sentido) for sentido in (
                    (extensao_ida, partidas_ida) as 'Ida',
                    (extensao_volta, partidas_volta) as 'Volta'
                )
            )

    )
select
    c.data,
    o.servico,
    o.sentido,
    o.extensao,
    datetime(c.data, parse_time("%H:%M:%S", "00:00:00")) as faixa_horaria_inicio,
    datetime(c.data, parse_time("%H:%M:%S", "23:59:59")) as faixa_horaria_fim,
    o.quilometragem,
    partidas_sentido,
    tipo_dia,
    tipo_os,
    feed_version,
    feed_start_date
{# from {{ ref("calendario") }} c #}
from `rj-smtr.planejamento.calendario` c
join os_sentido o using (feed_version, feed_start_date, tipo_dia, tipo_os)
