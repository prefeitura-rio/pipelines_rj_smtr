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

        {% set ordens_pagamento_modificadas_query %}
            select distinct concat("'", data_ordem, "'") from {{ transacao }} where data in ({{ transacao_partitions | join(", ") }}) and data_ordem is not null

            union distinct

            select distinct concat("'", data_ordem, "'") from {{ integracao }} where data in ({{ transacao_partitions | join(", ") }}) and data_ordem is not null
        {% endset %}
        {% if transacao_partitions_query | length > 0 %}
            {% set ordens_pagamento_modificadas = (
                run_query(ordens_pagamento_modificadas_query)
                .columns[0]
                .values()
            ) %}
        {% else %} {% set ordens_pagamento_modificadas = [] %}
        {% endif %}
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
            data_ordem,
            data as data_transacao,
            id_transacao,
            modo,
            consorcio,
            id_operadora,
            id_servico_jae,
            ifnull(sum(valor_rateio_compensacao), 0) as valor_transacao_rateio,
            id_ordem_pagamento,
            id_ordem_pagamento_consorcio as id_ordem_pagamento_consorcio_dia,
            id_ordem_pagamento_consorcio_operadora
            as id_ordem_pagamento_consorcio_operador_dia
        from {{ integracao }}
        {% if is_incremental() %}
            where
                {% if transacao_partitions | length > 0 %}
                    data in ({{ transacao_partitions | join(", ") }})
                {% else %} data = "2000-01-01"
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
    ),
    ordem_agrupada as (
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
            id_ordem_pagamento_consorcio_operador_dia
        from transacao_integracao
        where data_ordem is not null
        group by all
    ),
    particao_completa as (
        select *, 0 as priority
        from ordem_agrupada
        {% if is_incremental() and ordens_pagamento_modificadas | length > 0 %}
            union all
            select *, 1 as priority
            from {{ this }}
            where data_ordem in ({{ ordens_pagamento_modificadas | join(", ") }})
        {% endif %}
    )
select
    * except (priority),
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from particao_completa
qualify row_number() over (partition by id_transacao, data_ordem order by priority) = 1
