/*
Modelo cenário A
    - Considera todos os pares data-serviço cujo POD (Percentual de Operação Diária) esteja acima de 80%

Modelo cenário B
    - Considera apenas os pares data-serviço cujo POD (Percentual de Operação Diária) esteja acima de 80%,
      desconsiderando as faixas horárias com POF (Percentual de Operação por Faixa Horária) inferior a 80%.
    - Justificativa Técnica:
        - Na metodologia atualmente aplicada ao Encontro de Contas, ao se considerar todas as faixas horárias de um determinado par data-serviço,
          é possível que o POD final fique abaixo de 80%, o que resulta na exclusão desse par do encontro — mesmo que parte da quilometragem tenha
          sido efetivamente operada.
        - Vantagens do Cenário B:
            - Mitiga a perda de receita tarifária nos pares data-serviço com POD inferior a 80%;
            - Garante maior equilíbrio entre o que foi efetivamente operado e o que é considerado no Encontro de Contas.

Conclusão
    - Ambos cenários têm a mesma quantidade de pares data-serviço (129742), logo, não há diferença no resultado final.
*/
{# {% set subsidio_faixa_servico_dia_tipo_viagem = ref("subsidio_faixa_servico_dia_tipo_viagem") %} #}
{# {% set viagem_planejada = ref("viagem_planejada") %} #}
{# {% set sumario_faixa_servico_dia = ref("sumario_faixa_servico_dia") %} #}
{# {% set sumario_faixa_servico_dia_pagamento = ref("sumario_faixa_servico_dia_pagamento") %} #}
{% set subsidio_faixa_servico_dia_tipo_viagem = (
    "rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem"
) %}
{% set viagem_planejada = "rj-smtr.projeto_subsidio_sppo.viagem_planejada" %}
{% set sumario_faixa_servico_dia = (
    "rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia"
) %}
{% set sumario_faixa_servico_dia_pagamento = (
    "rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento"
) %}

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
            sum(
                case
                    when
                        data >= date("{{ var('DATA_SUBSIDIO_V9A_INICIO') }}")
                        and tipo_viagem not in ("Não licenciado", "Não vistoriado")
                    then km_apurada_faixa
                    when data < date("{{ var('DATA_SUBSIDIO_V9A_INICIO') }}")
                    then km_apurada_faixa
                    else 0
                end
            ) as km_apurada_faixa
        from {{ subsidio_faixa_servico_dia_tipo_viagem }}
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
        group by
            data, tipo_dia, consorcio, servico, faixa_horaria_inicio, faixa_horaria_fim
    ),
    sumario_faixa_servico_dia_v1 as (
        select
            sdp.data,
            sdp.tipo_dia,
            sdp.consorcio,
            sdp.servico,
            sum(sdp.viagens_faixa) as viagens_dia,
            sum(sfa.km_apurada_faixa) as km_apurada,
            sum(sdp.km_planejada_faixa) as km_planejada_dia,
            sum(sdp.valor_apurado) as valor_a_pagar,
        {# sum(sdp.valor_penalidade) as valor_penalidade #}
        from {{ sumario_faixa_servico_dia }} as sdp
        left join
            subsidio_faixa_agg as sfa using (
                data,
                servico,
                faixa_horaria_inicio,
                faixa_horaria_fim,
                tipo_dia,
                consorcio
            )
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
            and pof >= 80  -- Desabilitar para o cenário A
        group by data, tipo_dia, consorcio, servico
    ),
    sumario_faixa_servico_dia_v2 as (
        select
            sdp.data,
            sdp.tipo_dia,
            sdp.consorcio,
            sdp.servico,
            sum(sdp.viagens_faixa) as viagens_dia,
            sum(sfa.km_apurada_faixa) as km_apurada,
            sum(sdp.km_planejada_faixa) as km_planejada_dia,
            sum(sdp.valor_a_pagar) as valor_a_pagar,
        {# sum(sdp.valor_penalidade) as valor_penalidade #}
        from {{ sumario_faixa_servico_dia_pagamento }} as sdp
        left join
            subsidio_faixa_agg as sfa using (
                data,
                servico,
                faixa_horaria_inicio,
                faixa_horaria_fim,
                tipo_dia,
                consorcio
            )
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data >= date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
            and pof >= 80  -- Desabilitar para o cenário A
        group by data, tipo_dia, consorcio, servico
    ),
    valores_subsidio as (
        select *
        from sumario_faixa_servico_dia_v1
        union all
        select *
        from sumario_faixa_servico_dia_v2
    ),
    planejada as (
        select distinct data, consorcio, servico, vista
        from {{ viagem_planejada }}
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
        {# valor_penalidade #}
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
{# valor_penalidade #}
from pagamento
