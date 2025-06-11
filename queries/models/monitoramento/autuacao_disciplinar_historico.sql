{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

with
    autuacao_disciplinar_staging as (
        select
            data_infracao as data,
            id_auto_infracao,
            id_infracao,
            modo,
            servico,
            permissao,
            placa,
            valor,
            data_pagamento,
            date(data) as data_inclusao
        from {{ ref("staging_infracao") }}
        {% if is_incremental() %}
            where
                date(data) between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

        {% endif %}
        qualify row_number() over (partition by id_auto_infracao order by data desc) = 1
    ),
    aux_data_inclusao as (
        select id_auto_infracao, min(data_inclusao) as data_inclusao
        from
            {% if is_incremental() %}
                (
                    select id_auto_infracao, data_inclusao
                    from {{ this }}
                    union all
                    select id_auto_infracao, data_inclusao
                    from autuacao_disciplinar_staging
                )
            {% else %} autuacao_disciplinar_staging
            {% endif %}
        group by 1
    ),
    dados_novos as (
        select
            ad.* except (data_inclusao),
            di.data_inclusao,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from autuacao_disciplinar_staging ad
        join aux_data_inclusao di using (id_auto_infracao)
        where ad.data > "2025-03-31"
    )
{% if is_incremental() %}
        ,
        dados_completos as (
            select *, 'tratada' as fonte, 0 as ordem
            from {{ this }}
            union all
            select *, 'staging' as fonte, 1 as ordem
            from dados_novos
        ),
        dados_completos_sha as (
            {% set columns = (
                list_columns()
                | reject(
                    "in",
                    [
                        "versao",
                        "datetime_ultima_atualizacao",
                    ],
                )
                | list
            ) %}

            select
                *,
                sha256(
                    concat(
                        {% for c in columns %}
                            ifnull(cast({{ c }} as string), 'n/a')
                            {% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
                ) as sha_dado
            from dados_completos
        )
    select * except (sha_dado)
    from dados_completos_sha
    window win as (partition by id_auto_infracao order by ordem)
    qualify
        (
            fonte = 'tratada'
            and (
                lead(sha_dado) over (win) = sha_dado
                or lead(sha_dado) over (win) is null
            )
        )
        or (
            fonte = 'staging'
            and (
                lag(sha_dado) over (win) != sha_dado or lag(sha_dado) over (win) is null
            )
        )
{% else %} select * from dados_novos
{% endif %}
