{{
    config(
        materialized="table",
    )
}}

with
    linha_tarifa as (
        select
            cd_linha as id_servico_jae,
            vl_tarifa_ida as tarifa_ida,
            vl_tarifa_volta as tarifa_volta,
            lead(dt_inicio_validade) over (
                partition by cd_linha order by nr_sequencia
            ) as data_fim_validade
        from {{ ref("staging_linha_tarifa") }}
    ),
    linha_consorcio_operadora as (
        select * from {{ ref("staging_linha_consorcio_operadora_transporte") }}
    )
