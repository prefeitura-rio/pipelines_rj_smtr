{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        partition_by={
            "field": "datetime_captura",
            "data_type": "datetime",
            "granularity": "day",
        },
        cluster_by=["id_conta"],
        unique_key="id_lancamento_particao",
    )
}}

{% set staging_lancamento = ref("staging_lancamento") %}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

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

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    dados_novos as (
        select
            l.timestamp_captura as datetime_captura,
            datetime(datetime_captura) as data,
            cast(l.id_conta as string) as id_conta,
            l.cd_cliente as id_cliente,
            concat(
                cast(l.id_lancamento as string), "-", cast(l.id_conta as string)
            ) as id_lancamento_particao,
            j.nome as nome_cliente,
            j.documento as nr_documento,
            j.tipo_documento,
            l.id_lancamento,
            l.dt_lancamento as data_lancamento,
            l.vl_lancamento as valor_lancamento,
            l.ds_tipo_movimento as tipo_movimento,
            case
                when regexp_contains(l.id_conta, r'^2\.2\.3\.[A-Za-z0-9]+\.1$')
                then split(l.id_conta, ".")[offset(3)]
            end as nr_logico_midia
        from {{ ref("staging_lancamento") }} as l
        -- from `rj-smtr-dev.adriano__bilhetagem_interno_staging.lancamento`
        left join {{ ref("cliente_jae") }} j on j.id_cliente = l.cd_cliente
        where
            (
                regexp_contains(l.id_conta, r'^2\.2\.1\.[A-Za-z0-9]+\.(1|2|6)$')
                or regexp_contains(l.id_conta, r'^2\.2\.3\.[A-Za-z0-9]+\.1$')
            )
        qualify
            row_number() over (
                partition by l.id_lancamento, l.id_conta
                order by l.timestamp_captura desc
            )
            = 1
    ) sha_dados_novos as (select *, {{ sha_column }} as sha_dado_novo from dados_novos),
    sha_dados_atuais as (
        {% if is_incremental() and partitions | length > 0 %}

            select
                id_lancamento_particao,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }} as t
            join dados_novos as n using (id_lancamento_particao)

        {% else %}
            select
                cast(null as string) as id_lancamento_particao,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_lancamento_particao)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_lancamento_particao)
    ),
    extrato_colunas_controle as (
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
from extrato_colunas_controle
