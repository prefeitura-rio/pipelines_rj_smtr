{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_ordem",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set transacao = ref("transacao") %}
{% set integracao = ref("integracao") %}
{% set transacao_ordem = ref("aux_transacao_id_ordem_pagamento") %}

{% if execute %}
    {% if is_incremental() %}
        {% set transacao_partitions_query %}
            WITH particoes_transacao AS (
                SELECT
                    CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data_transacao
                FROM
                    `{{ transacao.database }}.{{ transacao.schema }}.INFORMATION_SCHEMA.PARTITIONS`
                WHERE
                    table_name = "{{ transacao.identifier }}"
                    AND partition_id != "__NULL__"
                    AND DATETIME(last_modified_time, "America/Sao_Paulo") BETWEEN DATETIME("{{var('date_range_start')}}") AND (DATETIME("{{var('date_range_end')}}"))
            ),
            particoes_integracao AS (
                SELECT
                    CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data_transacao
                FROM
                    `{{ integracao.database }}.{{ integracao.schema }}.INFORMATION_SCHEMA.PARTITIONS`
                WHERE
                    table_name = "{{ integracao.identifier }}"
                    AND partition_id != "__NULL__"
                    AND DATETIME(last_modified_time, "America/Sao_Paulo") BETWEEN DATETIME("{{var('date_range_start')}}") AND (DATETIME("{{var('date_range_end')}}"))
            ),
            particoes_transacao_ordem AS (
                SELECT
                    CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data_transacao
                FROM
                    `{{ transacao_ordem.database }}.{{ transacao_ordem.schema }}.INFORMATION_SCHEMA.PARTITIONS`
                WHERE
                    table_name = "{{ transacao_ordem.identifier }}"
                    AND partition_id != "__NULL__"
                    AND DATETIME(last_modified_time, "America/Sao_Paulo") BETWEEN DATETIME("{{var('date_range_start')}}") AND (DATETIME("{{var('date_range_end')}}"))
            )
            SELECT
                data_transacao
            FROM
                particoes_transacao
            WHERE
                data_transacao IS NOT NULL
            UNION DISTINCT
            SELECT
                data_transacao
            FROM
                particoes_integracao
            WHERE
                data_transacao IS NOT NULL
            UNION DISTINCT
            SELECT
                data_transacao
            FROM
                particoes_transacao_ordem
            WHERE
                data_transacao IS NOT NULL

        {% endset %}

        {% set transacao_partitions = (
            run_query(transacao_partitions_query).columns[0].values()
        ) %}

    {% endif %}
{% endif %}

with
    transacao as (
        select
            data_ordem,
            data as data_transacao,
            id_transacao,
            modo,
            consorcio,
            id_operadora,
            id_servico_jae,
            valor_transacao as valor_transacao_rateio,
            id_ordem_pagamento,
            id_ordem_pagamento_consorcio_dia,
            id_ordem_pagamento_consorcio_operador_dia
        from {{ ref("transacao") }}
        {% if is_incremental() %}
            where
                {% if transacao_partitions | length > 0 %}
                    data in ({{ transacao_partitions | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        {% endif %}
    ),
    integracao as (
        select
            o.data_ordem,
            i.data as data_transacao,
            i.id_transacao,
            i.modo,
            i.consorcio,
            i.id_operadora,
            i.id_servico_jae,
            ifnull(sum(i.valor_rateio_compensacao), 0) as valor_transacao_rateio,
            o.id_ordem_pagamento,
            o.id_ordem_pagamento_consorcio as id_ordem_pagamento_consorcio_dia,
            o.id_ordem_pagamento_consorcio_operadora
            as id_ordem_pagamento_consorcio_operador_dia
        {# ifnull(o.id_ordem_pagamento, t.id_ordem_pagamento) as id_ordem_pagamento,
            ifnull(
                o.id_ordem_pagamento_consorcio, t.id_ordem_pagamento_consorcio_dia
            ) as id_ordem_pagamento_consorcio_dia,
            ifnull(
                o.id_ordem_pagamento_consorcio_operadora,
                t.id_ordem_pagamento_consorcio_operador_dia
            ) as id_ordem_pagamento_consorcio_operador_dia #}
        from {{ integracao }} i
        left join {{ ref("staging_ordem_rateio") }} o using (id_ordem_rateio)
        join transacao t using (id_transacao)
        {% if is_incremental() %}
            where
                {% if transacao_partitions | length > 0 %}
                    i.data in ({{ transacao_partitions | join(", ") }})
                {% else %} i.data = "2000-01-01"
                {% endif %}
        {% endif %}
        group by all
    ),
    transacao_integracao as (
        select *
        from transacao
        union all
        select *
        from integracao
    )
select
    data_ordem,
    data_transacao,
    id_transacao,
    modo,
    consorcio,
    id_operadora,
    id_servico_jae,
    sum(valor_transacao_rateio) as valor_transacao_rateio,
    id_ordem_pagamento,
    id_ordem_pagamento_consorcio_dia,
    id_ordem_pagamento_consorcio_operador_dia,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from transacao_integracao
where data_ordem is not null
group by all
