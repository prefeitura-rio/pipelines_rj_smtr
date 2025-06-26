{# {% set sumario_servico_dia_historico = ref("monitoramento_sumario_servico_dia_historico") %} #}
{% set sumario_servico_dia_historico = (
    "rj-smtr.monitoramento.sumario_servico_dia_historico"
) %}

select *
from
    (
        select
            data,
            servico,
            consorcio,
            km_apurada as km_apurada_pod,
            km_apurada,
            km_planejada,
            perc_km_planejada,
            valor_subsidio_pago
        from {{ sumario_servico_dia_historico }}
        where data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
        union all
        select *
        from {{ ref("staging_encontro_contas_servico_dia") }}
        where data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
    )
where
    data >= "{{ var('encontro_contas_datas_v2_inicio') }}"
    and data between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
