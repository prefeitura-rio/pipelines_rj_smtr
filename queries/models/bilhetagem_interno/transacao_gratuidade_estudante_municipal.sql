{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set transacao = ref("transacao") %}
{% set cliente_jae = ref("cliente_jae") %}
{% set aux_gratuidade_info = ref("aux_gratuidade_info") %}


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

    {% set data_partitions_query %}
      SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
      FROM
        `{{ transacao.database }}.{{ transacao.schema }}.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        table_name = "{{ transacao.identifier }}"
        AND partition_id != "__NULL__"
        AND DATE(last_modified_time, "America/Sao_Paulo") = DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 1 DAY)

    {% endset %}

    {{ log("Running query: \n" ~ data_partitions_query, info=True) }}
    {% set partitions = run_query(data_partitions_query) %}

    {% set partition_list = partitions.columns[0].values() %}
    {{ log("transacao partitions: \n" ~ partition_list, info=True) }}

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    dados_novos as (
        select
            t.data,
            t.hora,
            t.datetime_transacao,
            t.id_transacao,
            t.modo,
            t.tipo_documento_cliente,
            t.documento_cliente,
            c.nome as nome_cliente,
            t.tipo_transacao,
            t.tipo_transacao_jae,
            t.tipo_usuario,
            t.subtipo_usuario,
            t.meio_pagamento,
            t.id_cre_escola,
            a.nome_escola
        from {{ ref("transacao") }} t
        join {{ ref("cliente_jae") }} c on t.id_cliente = c.id_cliente
        join
            {{ ref("aux_gratuidade_info") }} a
            on cast(a.id_cliente as string) = t.id_cliente
        where
            t.tipo_transacao_jae in ('Gratuidade', 'Integração gratuidade')
            and t.tipo_usuario = "Estudante"
            and t.subtipo_usuario = 'Ensino Básico Municipal'
            {% if is_incremental() %}
                {% if partition_list | length > 0 %}
                    and data in ({{ partition_list | join(", ") }})
                {% else %} and 1 = 0
                {% endif %}
            {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo
        from dados_novos
        qualify
            row_number() over (
                partition by id_transacao order by datetime_transacao desc
            )
            = 1
    )
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_transacao,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}

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
    transacao_gratuidade_estudante_municipal_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual
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
from transacao_gratuidade_estudante_municipal_controle
