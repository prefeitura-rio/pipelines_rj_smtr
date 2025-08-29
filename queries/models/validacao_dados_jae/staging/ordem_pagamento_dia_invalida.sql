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
    ordem_pagamento_consorcio_dia as (
        select
            data_ordem,
            id_ordem_pagamento,
            sum(quantidade_total_transacao) as quantidade_total_transacao,
            sum(valor_total_transacao_liquido) as valor_total_transacao_liquido
        from {{ ref("bilhetagem_consorcio_dia") }}
        {% if is_incremental() %}
            where data_ordem = date("{{var('run_date')}}")
        {% endif %}
        group by 1, 2
    ),
    ordem_pagamento_dia as (
        select
            data_ordem,
            id_ordem_pagamento,
            quantidade_total_transacao,
            valor_total_transacao_liquido
        from {{ ref("bilhetagem_dia") }}
        {% if is_incremental() %}
            where data_ordem = date("{{var('run_date')}}")
        {% endif %}
    ),
    indicadores as (
        select
            d.data_ordem,
            d.id_ordem_pagamento,
            d.quantidade_total_transacao,
            cd.quantidade_total_transacao as quantidade_total_transacao_agregacao,
            d.valor_total_transacao_liquido,
            cd.valor_total_transacao_liquido as valor_total_transacao_liquido_agregacao,
            round(cd.valor_total_transacao_liquido, 2)
            != round(d.valor_total_transacao_liquido, 2)
            or cd.quantidade_total_transacao
            != d.quantidade_total_transacao as indicador_agregacao_invalida
        from ordem_pagamento_dia d
        left join
            ordem_pagamento_consorcio_dia cd using (data_ordem, id_ordem_pagamento)
    )
select *, '{{ var("version") }}' as versao
from indicadores
where indicador_agregacao_invalida = true
