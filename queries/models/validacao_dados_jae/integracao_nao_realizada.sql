{{
    config(
        materilized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set aux_particao_calculo_integracao = ref("aux_particao_calculo_integracao") %}

{% if execute and is_incremental() %}

    {% set partitions_query %}
        select particao from {{ aux_particao_calculo_integracao }} where tipo_particao = 'modificada'

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% set adjacent_partitions_query %}
        select particao from {{ aux_particao_calculo_integracao }} where tipo_particao = 'adjacente'

    {% endset %}

    {% set adjacent_partitions = (
        run_query(adjacent_partitions_query).columns[0].values()
    ) %}

{% endif %}

with
    integracao_transferencia_calculada as (
        select *
        from {{ ref("aux_integracao_calculada") }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %}

                    data in ({{ partitions | join(", ") }})
                    or data in ({{ adjacent_partitions | join(", ") }})

                {% else %} false
                {% endif %}
        {% endif %}
        qualify
            max(date(datetime_processamento)) over (partition by id_integracao)
            < current_date("America/Sao_Paulo")
            {% if is_incremental() and partitions | length > 0 %}
                and (
                    date(max(datetime_transacao) over (partition by id_integracao))
                    in ({{ partitions | join(", ") }})
                    or date(min(datetime_transacao) over (partition by id_integracao))
                    in ({{ partitions | join(", ") }})
                )
            {% endif %}
    ),
    integracao_jae as (
        select
            id_integracao,
            string_agg(id_transacao, '_' order by sequencia_integracao) as id_join
        from {{ ref("integracao") }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %}

                    data in ({{ partitions | join(", ") }})
                    or data in ({{ adjacent_partitions | join(", ") }})

                {% else %} false
                {% endif %}
        {% endif %}
        group by 1
    ),
    particao_completa as (
        select *
        from integracao_transferencia_calculada

        {% if is_incremental() and partitions | length > 0 %}
            union all

            select o.* except (versao, datetime_ultima_atualizacao, id_execucao_dbt)
            from {{ this }} o
            left join integracao_transferencia_calculada n using (id_transacao)
            where
                o.data in ({{ partitions | join(", ") }})
                or o.data in ({{ adjacent_partitions | join(", ") }})
            qualify max(n.id_transacao is null) over (partition by id_integracao)

        {% endif %}
    ),
    integracao_calculada_id_join as (
        select
            id_integracao,
            string_agg(id_transacao, '_' order by sequencia_integracao) as id_join
        from particao_completa
        group by 1
    ),
    integracao_nao_realizada as (
        select
            pc.*,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            '{{ invocation_id }}' as id_execucao_dbt
        from particao_completa pc
        join integracao_calculada_id_join id using (id_integracao)
        left join integracao_jae j using (id_join)
        where j.id_join is null
    )
select *
from integracao_nao_realizada
