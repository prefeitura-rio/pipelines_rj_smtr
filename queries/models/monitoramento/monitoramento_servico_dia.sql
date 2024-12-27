{{ config(materialized="ephemeral") }}

select
    data,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens,
    km_apurada,
    km_planejada,
    perc_km_planejada,
    valor_subsidio_pago,
    valor_penalidade
from -- {{ ref("sumario_servico_dia_historico") }}
`rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico`
where
    data < DATE("{{ var("DATA_SUBSIDIO_V9_INICIO") }}") --noqa
    {# {% if is_incremental() %} #}
        AND data BETWEEN DATE("{{ var("start_date") }}")
        AND DATE_ADD(DATE("{{ var("end_date") }}"), INTERVAL 1 DAY)
    {# {% endif %} #}
