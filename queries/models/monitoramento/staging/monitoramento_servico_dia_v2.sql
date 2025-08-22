{% set is_disabled = var("start_date") < var("DATA_SUBSIDIO_V14_INICIO") %}

{{ config(materialized="ephemeral") }}

with
    subsidio_faixa_agg as (
        select
            data,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            tipo_dia,
            consorcio,
            servico,
            sentido,
            sum(km_apurada_faixa) as km_apurada_faixa
        -- from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
        from `rj-smtr-dev.victor__financeiro.subsidio_faixa_servico_dia_tipo_viagem`
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
        group by
            data,
            tipo_dia,
            consorcio,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim
    ),
    sumario_faixa_servico_dia as (
        select
            sdp.data,
            sdp.tipo_dia,
            sdp.consorcio,
            sdp.servico,
            sum(sdp.viagens_faixa) as viagens_dia,
            sum(sfa.km_apurada_faixa) as km_apurada,
            sum(sdp.km_planejada_faixa) as km_planejada_dia,
            sum(sdp.valor_a_pagar) as valor_a_pagar,
            sum(sdp.valor_penalidade) as valor_penalidade
        from  -- {{ ref("sumario_faixa_servico_dia_pagamento") }} as sdp
            `rj-smtr-dev.victor__dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento`
            as sdp
        left join
            subsidio_faixa_agg as sfa
            on sdp.data = sfa.data
            and sdp.servico = sfa.servico
            and (
                sdp.sentido = sfa.sentido or sdp.sentido is null and sfa.sentido is null
            )
            and sdp.faixa_horaria_inicio = sfa.faixa_horaria_inicio
            and sdp.faixa_horaria_fim = sfa.faixa_horaria_fim
            and sdp.tipo_dia = sfa.tipo_dia
            and sdp.consorcio = sfa.consorcio
        where
            sdp.data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and sdp.data >= date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
        group by data, tipo_dia, consorcio, servico
    ),
    {% if is_disabled %}
        sumario_servico_dia as (
            select
                data,
                sdp.tipo_dia,
                sdp.consorcio,
                servico,
                sdp.viagens_dia,
                sum(km_apurada_faixa) as km_apurada,
                km_planejada_dia,
                valor_a_pagar,
                valor_penalidade
            from
                `rj-smtr-dev.victor__financeiro.subsidio_sumario_servico_dia_pagamento` as sdp
            left join subsidio_faixa_agg using (data, servico)
            where
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
                and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
            group by
                data,
                tipo_dia,
                consorcio,
                servico,
                viagens_dia,
                km_planejada_dia,
                valor_a_pagar,
                valor_penalidade
        ),
    {% endif %}
    valores_subsidio as (
        {% if is_disabled %}
            select *
            from sumario_servico_dia
            union all
        {% endif %}
        select *
        from sumario_faixa_servico_dia
    ),
    planejada as (
        select distinct data, consorcio, servico, vista
        from  -- {{ ref("viagem_planejada") }}
            `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
        where
            data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
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
            km_apurada,
            km_planejada_dia as km_planejada,
            valor_a_pagar as valor_subsidio_pago,
            valor_penalidade
        from valores_subsidio as sdp
        left join planejada as p using (data, servico, consorcio)
        where
            data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
            {% if is_incremental() %}
                and data between date("{{ var('start_date') }}") and date_add(
                    date("{{ var('end_date') }}"), interval 1 day
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
    round(100 * km_apurada / km_planejada, 2) as perc_km_planejada,
    valor_subsidio_pago,
    valor_penalidade
from pagamento
