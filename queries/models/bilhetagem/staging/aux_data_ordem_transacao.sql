{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_ordem",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}

            select distinct concat("'", data, "'") as data_transacao
            from {{ ref("aux_transacao_id_ordem_pagamento") }}
            where {{ incremental_filter }}

        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% endif %}
{% endif %}


with
    aux as (
        select distinct
            data_ordem,
            id_ordem_pagamento_consorcio_operador_dia,
            data as data_transacao
        from {{ ref("aux_transacao_id_ordem_pagamento") }}
        {% if is_incremental() %}
            where data in ({{ partitions | join(", ") }})
        {% endif %}
    )
select
    data_ordem,
    id_ordem_pagamento_consorcio_operador_dia,
    data_transacao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from aux
