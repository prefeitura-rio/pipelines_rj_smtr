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

{% set transacao_ordem = ref("aux_transacao_ordem") %}
{% if execute and is_incremental() %}
    {% set transacao_validacao_partition_query %}
    SELECT DISTINCT
      CONCAT("'", DATE(data_transacao), "'") AS data_transacao
    FROM
      {{ transacao_ordem }}
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
    {% endset %}
    {% set transacao_validacao_partitions = (
        run_query(transacao_validacao_partition_query).columns[0].values()
    ) %}
{% endif %}

with
    transacao_invalida as (
        select id_transacao, indicador_servico_fora_vigencia
        from {{ ref("transacao_invalida") }}
        where
            indicador_servico_fora_vigencia = true
            {% if is_incremental() %}
                and {% if transacao_validacao_partitions | length > 0 %}
                    data in ({{ transacao_validacao_partitions | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
            {% endif %}
    ),
    transacao_agg as (
        select
            t.data_ordem,
            any_value(t.id_consorcio) as id_consorcio,
            t.id_servico_jae,
            t.id_operadora,
            count(*) as quantidade_total_transacao_captura,
            sum(t.valor_transacao) as valor_total_transacao_captura,
            max(ti.indicador_servico_fora_vigencia)
            is not null as indicador_servico_fora_vigencia
        from {{ ref("aux_transacao_ordem") }} t
        left join transacao_invalida ti using (id_transacao)
        group by data_ordem, id_servico_jae, id_operadora
    ),
    ordem_pagamento as (
        select *
        from {{ ref("bilhetagem_servico_operador_dia") }}
        {% if is_incremental() %}
            where data_ordem = date("{{var('run_date')}}")
        {% endif %}
    ),
    id_ordem_pagamento as (
        select data_ordem, id_ordem_pagamento
        from {{ ref("ordem_pagamento_dia") }}
        {% if is_incremental() %}
            where data_ordem = date("{{var('run_date')}}")
        {% endif %}
    ),
    transacao_ordem as (
        select
            coalesce(op.data_ordem, t.data_ordem) as data_ordem,
            coalesce(op.id_consorcio, t.id_consorcio) as id_consorcio,
            coalesce(op.id_operadora, t.id_operadora) as id_operadora,
            coalesce(op.id_servico_jae, t.id_servico_jae) as id_servico_jae,
            op.quantidade_total_transacao,
            op.valor_total_transacao_bruto,
            op.valor_total_transacao_liquido,
            t.quantidade_total_transacao_captura,
            safe_cast(
                t.valor_total_transacao_captura
                + op.valor_rateio_credito
                + op.valor_rateio_debito as numeric
            ) as valor_total_transacao_captura,
            t.indicador_servico_fora_vigencia
        from ordem_pagamento op
        full outer join transacao_agg t using (data_ordem, id_servico_jae, id_operadora)
    ),
    indicadores as (
        select
            o.data_ordem,
            id.id_ordem_pagamento,
            o.id_consorcio,
            o.id_operadora,
            o.id_servico_jae,
            o.quantidade_total_transacao,
            o.valor_total_transacao_bruto,
            o.valor_total_transacao_liquido,
            o.quantidade_total_transacao_captura,
            o.valor_total_transacao_captura,
            coalesce(
                (
                    quantidade_total_transacao_captura != quantidade_total_transacao
                    or round(valor_total_transacao_captura, 2)
                    != round(valor_total_transacao_bruto, 2)
                ),
                true
            ) as indicador_captura_invalida,
            o.indicador_servico_fora_vigencia
        from transacao_ordem o
        join id_ordem_pagamento id using (data_ordem)
    )
select *, '{{ var("version") }}' as versao
from indicadores
where
    indicador_servico_fora_vigencia = true
    or indicador_captura_invalida = true
    and id_servico_jae
    not in (select id_linha from {{ ref("staging_linha_sem_ressarcimento") }})
