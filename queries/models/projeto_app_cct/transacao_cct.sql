{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set transacao = ref("transacao") %}


{% set transacao_filter %}
    id_ordem_pagamento_consorcio_operador_dia is not null
    and modo = 'Van'
    and t.consorcio in ('STPC', 'STPL', 'TEC')
    and t.valor_pagamento > 0
{% endset %}

-- busca quais partições serão atualizadas pelas capturas
{% if execute and is_incremental() %}

    {% set columns = (
        list_columns()
        | reject(
            "in",
            ["versao", "datetime_ultima_atualizacao", "id_execucao_dbt"],
        )
        | list
    ) %}
    {% set sha_column %}
        sha256(
            concat(
                {% for c in columns %}
                    ifnull(cast({{ c }} as string), 'n/a')
                    {% if not loop.last %}, {% endif %}

                {% endfor %}
            )
        )
    {% endset %}

    {% set transacao_partitions_query %}
            select
                concat("'", parse_date("%Y%m%d", partition_id), "'") as particao
            from
                `{{ transacao.database }}.{{ transacao.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ transacao.identifier }}"
                and partition_id != "__NULL__"
                and
                    datetime(last_modified_time, "America/Sao_Paulo")
                    between datetime("{{var('date_range_start')}}")
                    and datetime("{{var('date_range_end')}}")

    {% endset %}

    {% set transacao_partitions = (
        run_query(transacao_partitions_query).columns[0].values()
    ) %}

    {% if transacao_partitions | length > 0 %}
        {% set partitions_query %}
            select distinct data_ordem
            from {{ transacao }}
            where
                data in ({{ transacao_partitions | join(", ") }}) and {{ transacao_filter }}
        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}
    {% else %} {% set partitions = [] %}
    {% endif %}

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    dados_novos as (
        select
            data_ordem,
            id_transacao,
            data,
            datetime_transacao,
            consorcio,
            tipo_transacao_jae as tipo_transacao,
            meio_pagamento_jae as tipo_pagamento,
            round(t.valor_pagamento, 2) valor_pagamento,
            round(t.valor_transacao, 2) valor_transacao,
            id_ordem_pagamento,
            id_ordem_pagamento_consorcio_operador_dia
        from {{ ref("transacao") }}
        where
            {{ transacao_filter }}
            {% if is_incremental() %}
                and {% if transacao_partitions | length > 0 %}
                    data in ({{ transacao_partitions | join(", ") }})
                {% else %} false
                {% endif %}
            {% else %} data >= {{ var("data_inicial_transacao") }}
            {% endif %}
    ),
    {% if is_incremental() %}

        dados_atuais as (
            select *
            from {{ this }}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} false
                {% endif %}

        ),
    {% endif %}
    particoes_completas as (
        select *, 0 as priority
        from dados_novos

        {% if is_incremental() %}
            union all

            select
                * except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais

        {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo
        from particoes_completas
        qualify
            row_number() over (
                partition by id_transacao order by priority, datetime_transacao desc
            )
            = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_transacao
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_transacao,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_transacao)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_transacao)
    ),
    colunas_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual,
                priority
            ),
            '{{ var("version") }}' as versao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then current_datetime("America/Sao_Paulo")
                else datetime_ultima_atualizacao_atual
            end as datetime_ultima_atualizacao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then '{{ invocation_id }}'
                else id_execucao_dbt_atual
            end as id_execucao_dbt
        from sha_dados_completos
    )
select *
from colunas_controle
