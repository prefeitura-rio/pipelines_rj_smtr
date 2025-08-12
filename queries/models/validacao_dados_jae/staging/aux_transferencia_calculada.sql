{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set max_pernas = 6 %}
{% set aux_transacao_possivel_transferencia = ref(
    "aux_transacao_possivel_transferencia"
) %}

{% if execute and is_incremental() %}

    {% set partitions_query %}

        select distinct concat("'", data, "'") from {{aux_transacao_possivel_transferencia}}

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

with
    transacao as (select * from {{ aux_transacao_possivel_transferencia }}),
    particao_completa as (
        select *
        from transacao

        {% if is_incremental() and partitions | length > 0 %}
            union all by name

            select *
            from {{ this }}
            where
                data in ({{ partitions | join(", ") }})
                and id_transferencia not in (select id_transferencia from transacao)
        {% endif %}
    ),
    {% for i in range(max_pernas + 1) %}
        {% if i == 0 %} {% set tabela_dados = "particao_completa" %}
        {% else %}
            {% set last_i = i | int - 1 %} {% set tabela_dados = "remocao_" ~ last_i %}
        {% endif %}

        indicador_remover_{{ i }} as (
            select
                *,
                row_number() over (
                    partition by id_transacao order by datetime_inicio_transferencia
                )
                > 1 as indicador_remover
            from {{ tabela_dados }}
        ),
        max_indicador_remover_{{ i }} as (
            select
                * except (indicador_remover),
                max(indicador_remover) over (
                    partition by id_transferencia
                ) as indicador_remover,
            from indicador_remover_{{ i }}
        ),
        validacao_{{ i }} as (
            select * except (indicador_remover),
            from max_indicador_remover_{{ i }}
            qualify
                lag(indicador_remover) over (
                    partition by cliente_cartao order by datetime_inicio_transferencia
                )
                is false
                and indicador_remover is true
        ),
        remocao_{{ i }} as (
            select i.*
            from {{ tabela_dados }} i
            left join validacao_{{ i }} v using (id_transferencia)
            where v.id_transferencia is null
        ),
    {% endfor %}
    final as (select * from remocao_{{ max_pernas }})
select *
from final
