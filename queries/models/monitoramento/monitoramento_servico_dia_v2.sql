{{ config(materialized="ephemeral") }}

with
    planejada as (
        select distinct data, consorcio, servico, vista
        from {{ ref("viagem_planejada") }}
        -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
        where
            data >= date("{{ var("DATA_SUBSIDIO_V9_INICIO") }}")
            and (id_tipo_trajeto = 0 or id_tipo_trajeto is null)
            and format_time("%T", time(faixa_horaria_inicio)) != "00:00:00"
    ),
    pagamento as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            vista,
            viagens_dia as viagens,
            case
                when data >= date("{{ var("DATA_SUBSIDIO_V9A_INICIO") }}")
                then
                    coalesce(km_apurada_registrado_com_ar_inoperante, 0)
                    + coalesce(km_apurada_autuado_ar_inoperante, 0)
                    + coalesce(km_apurada_autuado_seguranca, 0)
                    + coalesce(km_apurada_autuado_limpezaequipamento, 0)
                    + coalesce(km_apurada_licenciado_sem_ar_n_autuado, 0)
                    + coalesce(km_apurada_licenciado_com_ar_n_autuado, 0)
                    + coalesce(km_apurada_sem_transacao, 0)
                else
                    coalesce(km_apurada_registrado_com_ar_inoperante, 0)
                    + coalesce(km_apurada_autuado_ar_inoperante, 0)
                    + coalesce(km_apurada_autuado_seguranca, 0)
                    + coalesce(km_apurada_autuado_limpezaequipamento, 0)
                    + coalesce(km_apurada_licenciado_sem_ar_n_autuado, 0)
                    + coalesce(km_apurada_licenciado_com_ar_n_autuado, 0)
                    + coalesce(km_apurada_sem_transacao, 0)
                    + coalesce(km_apurada_n_vistoriado, 0)
                    + coalesce(km_apurada_n_licenciado, 0)
            end as km_apurada,
            km_planejada_dia as km_planejada,
            valor_a_pagar as valor_subsidio_pago,
            valor_penalidade
        from {{ ref("subsidio_sumario_servico_dia_pagamento") }} as sdp
        --`rj-smtr.dashboard_subsidio_sppo_v2.sumario_servico_dia_pagamento`
        left join planejada as p using (data, servico, consorcio)
        where
            data >= date("{{ var("DATA_SUBSIDIO_V9_INICIO") }}")
            {% if is_incremental() %}
                and data between date("{{ var("start_date") }}") and date_add(
                    date("{{ var("end_date") }}"), interval 1 day
                )
            {% endif %}
    )
select
    data,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens,
    km_apurada,
    km_planejada,
    100 * km_apurada / km_planejada as perc_km_planejada,
    valor_subsidio_pago,
    valor_penalidade
from pagamento
