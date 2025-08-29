-- depends_on: {{ ref("ordem_pagamento_servico_operador_dia_invalida") }}
{{
    config(
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_ordem",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    ordem_pagamento_servico_operador_dia as (
        select
            data_ordem,
            id_consorcio,
            id_operadora,
            id_ordem_pagamento,
            sum(quantidade_total_transacao) as quantidade_total_transacao,
            sum(valor_total_transacao_liquido) as valor_total_transacao_liquido,
        from {{ ref("bilhetagem_servico_operador_dia") }}
        {% if is_incremental() %}
            where data_ordem = date("{{var('run_date')}}")
        {% endif %}
        group by 1, 2, 3, 4
    ),
    ordem_pagamento_consorcio_operador_dia as (
        select
            data_ordem,
            id_consorcio,
            id_operadora,
            id_ordem_pagamento,
            quantidade_total_transacao,
            valor_total_transacao_liquido_ordem as valor_total_transacao_liquido
        from {{ ref("bilhetagem_consorcio_operador_dia") }}
        {% if is_incremental() %}
            where data_ordem = date("{{var('run_date')}}")
        {% endif %}
    ),
    indicadores as (
        select
            cod.data_ordem,
            cod.id_consorcio,
            cod.id_operadora,
            cod.id_ordem_pagamento,
            cod.quantidade_total_transacao,
            sod.quantidade_total_transacao as quantidade_total_transacao_agregacao,
            cod.valor_total_transacao_liquido,
            sod.valor_total_transacao_liquido
            as valor_total_transacao_liquido_agregacao,
            round(cod.valor_total_transacao_liquido, 2)
            != round(sod.valor_total_transacao_liquido, 2)
            or cod.quantidade_total_transacao
            != sod.quantidade_total_transacao as indicador_agregacao_invalida
        from ordem_pagamento_consorcio_operador_dia cod
        left join
            ordem_pagamento_servico_operador_dia sod using (
                data_ordem, id_consorcio, id_operadora, id_ordem_pagamento
            )
    )
select *, '{{ var("version") }}' as versao
from indicadores
where indicador_agregacao_invalida = true
