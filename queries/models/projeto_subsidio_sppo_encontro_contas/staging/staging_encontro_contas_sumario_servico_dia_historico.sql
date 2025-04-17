{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        alias="sumario_servico_dia_historico",
    )
}}

{# {% set sumario_servico_dia_historico = ref("monitoramento_sumario_servico_dia_historico") %} #}
{% set sumario_servico_dia_historico = (
    "rj-smtr.monitoramento.sumario_servico_dia_historico"
) %}

select *
from
    (
        select * except (valor_penalidade)
        from {{ sumario_servico_dia_historico }}
        where data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
        union all
        select *
        from {{ ref("staging_encontro_contas_servico_dia") }}
        where data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
    )
where data between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
