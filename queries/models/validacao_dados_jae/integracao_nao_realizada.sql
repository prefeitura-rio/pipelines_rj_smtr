{{
    config(
        materilized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set aux_integracao_calculada = ref("aux_integracao_calculada") %}

{% if execute and is_incremental() %}
    {% set partitions_query %}
        select
            concat("'", parse_date("%Y%m%d", partition_id), "'") as particao
            {# concat("'", parse_date("%Y%m%d", partition_id), "'") as particao #}
        from
            `{{ aux_integracao_calculada.database }}.{{ aux_integracao_calculada.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        where
            table_name = "{{ aux_integracao_calculada.identifier }}"
            and partition_id != "__NULL__"
            and datetime(last_modified_time, "America/Sao_Paulo") between datetime("{{var('date_range_start')}}") and (datetime("{{var('date_range_end')}}"))

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

with
    integracao_calculada as (
        select
            *,
            string_agg(id_transacao, '_') over (
                partition by id_integracao order by sequencia_integracao
            ) as id_join
        from {{ ref("aux_integracao_calculada") }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} false
                {% endif %}
        {% endif %}
        qualify
            max(date(datetime_processamento)) over (partition by id_integracao)
            < current_date("America/Sao_Paulo")
    ),
    integracao_jae as (
        select
            id_integracao,
            string_agg(id_transacao, '_') over (
                partition by id_integracao order by sequencia_integracao
            ) as id_join
        from {{ ref("integracao") }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} false
                {% endif %}
        {% endif %}
    ),
    dados_novos as (
        select c.*, j.id_integracao is null as indicador_nao_realizado
        from integracao_calculada c
        left join integracao_jae j using (id_join)
    ),
    particao_completa as (
        select * except (id_join), 0 as priority
        from dados_novos

        {% if is_incremental() and partitions | length > 0 %}
            union all

            select *, true as indicador_nao_realizado, 1 as priority
            from {{ this }}
            where data in ({{ partitions | join(", ") }})

        {% endif %}
    ),
    particao_rn as (
        select
            *,
            row_number() over (
                partition by id_transacao order by datetime_inicio_integracao, priority
            ) as rn
        from particao_completa
    ),
    particao_filtrada as (
        select * except (rn, priority)
        from particao_rn
        qualify max(rn) over (partition by id_integracao, priority) = 1
    ),
    integracao_nao_realizada as (
        select
            * except (indicador_nao_realizado),
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            '{{ invocation_id }}' as id_execucao_dbt
        from particao_filtrada
        where indicador_nao_realizado
    )
select *
from integracao_nao_realizada
