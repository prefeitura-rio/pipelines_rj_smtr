-- depends_on: {{ ref("ordem_pagamento_consorcio_operador_dia_invalida") }}
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
    ordem_pagamento_consorcio_operador_dia as (
        select
            data_ordem,
            id_consorcio,
            id_ordem_pagamento,
            sum(quantidade_total_transacao) as quantidade_total_transacao,
            sum(valor_total_transacao_liquido_ordem) as valor_total_transacao_liquido,
        from {{ ref("bilhetagem_consorcio_operador_dia") }}
        {% if is_incremental() %}
            where data_ordem = date("{{var('run_date')}}")
        {% endif %}
        group by 1, 2, 3
    ),
    ordem_pagamento_consorcio_dia as (
        select
            data_ordem,
            id_consorcio,
            id_ordem_pagamento,
            quantidade_total_transacao,
            valor_total_transacao_liquido
        from {{ ref("bilhetagem_consorcio_dia") }}
        {% if is_incremental() %}
            where data_ordem = date("{{var('run_date')}}")
        {% endif %}
    ),
    indicadores as (
        select
            cd.data_ordem,
            cd.id_consorcio,
            cd.id_ordem_pagamento,
            cd.quantidade_total_transacao,
            cod.quantidade_total_transacao as quantidade_total_transacao_agregacao,
            cd.valor_total_transacao_liquido,
            cod.valor_total_transacao_liquido
            as valor_total_transacao_liquido_agregacao,
            round(cd.valor_total_transacao_liquido, 2)
            != round(cod.valor_total_transacao_liquido, 2)
            or cd.quantidade_total_transacao
            != cod.quantidade_total_transacao as indicador_agregacao_invalida
        from ordem_pagamento_consorcio_dia cd
        left join
            ordem_pagamento_consorcio_operador_dia cod using (
                data_ordem, id_consorcio, id_ordem_pagamento
            )
    )
select *, '{{ var("version") }}' as versao
from indicadores
where indicador_agregacao_invalida = true
