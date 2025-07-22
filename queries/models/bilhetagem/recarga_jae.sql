{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% if is_incremental() %}

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

{% else %} {% set sha_column = "cast(null as bytes)" %}
{% endif %}

with
    lancamento_staging as (
        select *
        from {{ ref("staging_lancamento") }}
        {% if is_incremental() %}
            where
                (
                    {{
                        generate_date_hour_partition_filter(
                            var("date_range_start"), var("date_range_end")
                        )
                    }}
                )
                and timestamp_captura
                between datetime("{{var('date_range_start')}}") and datetime(
                    "{{var('date_range_end')}}"
                )
        {% endif %}
    ),
    dados_novos as (
        select
            date(dt_lancamento) as data,
            id_lancamento as id_recarga,
            dt_lancamento as datetime_recarga,
            cd_cliente as id_cliente,
            id_conta,
            ds_tipo_conta as tipo_conta,
            ds_tipo_movimento as tipo_movimento,
            vl_lancamento as valor_recarga,
            tipo_moeda
        from lancamento_staging
        where cd_tipo_movimento in ('23', '24', '41') and vl_lancamento > 0
    ),
    {% if is_incremental() %}

        dados_atuais as (
            select *
            from {{ this }}
            where
                data
                between date(
                    datetime_sub(
                        datetime("{{var('date_range_start')}}"), interval 5 minutes
                    )
                ) and date(
                    datetime_sub(
                        datetime("{{var('date_range_end')}}"), interval 5 minutes
                    )
                )
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
        select *, {{ sha_column }} as sha_dado_novo from particoes_completas
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_recarga,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_recarga,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_recarga)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_recarga)
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
