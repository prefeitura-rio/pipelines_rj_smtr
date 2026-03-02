{{
    config(
        materialized="view",
    )
}}

/*
Descrição: Registra multas por não operação da linha (não considerado)
*/
with
    multa_120_minutos as (
        select
            *,
            countif(erro is not null) over (
                partition by data
                order by data
                rows between 60 preceding and 60 following
            )
            >= 120 flag_multa,
            "120 minutos com a API fora do ar" tipo_multa,
            "017.X" artigo_multa
        from `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_logs`
    ),
    multa_1_dia as (
        select
            *,
            countif(erro is not null) over (
                partition by data
                order by data
                rows between 720 preceding and 720 following
            )
            >= 720 flag_multa,
            "1 dia com a API fora do ar" tipo_multa,
            "017.IX" artigo_multa
        from `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_logs`
    )
select *
from multa_120_minutos
where flag_multa = true
union all
select *
from multa_1_dia
where flag_multa = true
