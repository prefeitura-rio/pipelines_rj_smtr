/*
- Modelo cenário A
    - Considera todos os pares data-serviço cujo POD (Percentual de Operação Diária) esteja igual ou superior a 80%.

- Modelo cenário B
    - Considera apenas os pares data-serviço com POD ≥ 80%, desconsiderando as faixas horárias em que o POF (Percentual de Operação por Faixa Horária)
      esteja abaixo de 80%.

- Modelo cenário C
    - Considera apenas os pares data-serviço com na qual nenhuma faixa possui POF < 80%

- Justificativa Técnicas
    - Na metodologia atualmente adotada no Encontro de Contas, ao considerar todas as faixas horárias de um par data-serviço, o POD agregado pode ficar
      abaixo de 80%, levando à exclusão desse par — mesmo que parte da quilometragem tenha sido efetivamente operada.
    - O Cenário B propõe uma abordagem mais refinada, ao eliminar apenas as faixas com POF inferior a 80%, mantendo no cálculo os trechos que atenderam
      aos critérios de desempenho e subsídio.
    - Vantagens do Cenário B:
        - Reduz a perda de receita tarifária decorrente da exclusão de pares com POD médio inferior a 80%, mas com operação parcial válida;
        - Promove maior alinhamento entre a operação efetivamente realizada e os critérios de elegibilidade para o Encontro de Contas;
        - Aumenta a precisão da compensação ao considerar apenas as faixas que atendem aos critérios de subsídio.

- Conclusão
    - Ambos cenários têm a mesma quantidade de pares data-serviço (129742);
    - No entanto, o Cenário B representa uma alternativa metodológica mais criteriosa e vantajosa do ponto de vista da gestão pública, ao evitar distorções
      causadas por faixas horárias não elegíveis ao subsídio.
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
            data,
            tipo_dia,
            consorcio,
            servico,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            viagens_faixa,
            km_apurada_faixa,
            km_planejada_faixa,
            valor_apurado as valor_a_pagar,
            pof
        from {{ sumario_faixa_servico_dia }}
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
    ),
    sumario_faixa_servico_dia_v2 as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            viagens_faixa,
            km_apurada_faixa,
            km_planejada_faixa,
            valor_a_pagar,
            pof
        {# sum(sdp.valor_penalidade) as valor_penalidade #}
        from {{ sumario_faixa_servico_dia_pagamento }} as sdp
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data >= date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
    ),
    sumario_faixa_servico_dia_v1_agg as (
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
        from sumario_faixa_servico_dia_v1 as sdp
        left join
            subsidio_faixa_agg as sfa using (
                data,
                servico,
                faixa_horaria_inicio,
                faixa_horaria_fim,
                tipo_dia,
                consorcio
            )
        {# where pof >= 80  -- Habilitar para o cenário B #}
        group by data, tipo_dia, consorcio, servico
    ),
    sumario_faixa_servico_dia_v2_agg as (
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
        from sumario_faixa_servico_dia_v2 as sdp
        left join
            subsidio_faixa_agg as sfa using (
                data,
                servico,
                faixa_horaria_inicio,
                faixa_horaria_fim,
                tipo_dia,
                consorcio
            )
        {# where and pof >= 80  -- Habilitar para o cenário B #}
        group by data, tipo_dia, consorcio, servico
    ),
    valores_subsidio as (
        select *
        from sumario_faixa_servico_dia_v1_agg
        union all
        select *
        from sumario_faixa_servico_dia_v2_agg
    ),
    planejada as (
        select distinct data, consorcio, servico, vista
        from {{ viagem_planejada }}
        where
            data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
            and (id_tipo_trajeto = 0 or id_tipo_trajeto is null)
            and format_time("%T", time(faixa_horaria_inicio)) != "00:00:00"
    ),
    -- Identifica pares data-serviço com ao menos uma faixa com POF < 80
    pof_inferior_80 as (
        select distinct data, servico
        from
            (
                select *
                from sumario_faixa_servico_dia_v1
                union all
                select *
                from sumario_faixa_servico_dia_v2
            )
        where pof < 80
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
        left join pof_inferior_80 as i using (data, servico)
        where
            data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}") and i.data is null  -- Cenário C: remove pares data-servico na qual uma das faixas teve POF < 80%
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
