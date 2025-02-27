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
    valor_subsidio_pago + coalesce(valor_penalidade, 0) as valor_subsidio_pago,
    coalesce(valor_penalidade, 0) as valor_penalidade
from {{ ref("sumario_servico_dia_historico") }}
    -- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico`
where
    data < date("{{ var('data_subsidio_v9_inicio') }}")  -- noqa
    {% if is_incremental() %}
        and data between date("{{ var('start_date') }}") and date_add(
            date("{{ var('end_date') }}"), interval 1 day
        )
    {% endif %}
