{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

/*
consulta as partições a serem atualizadas com base nas transações capturadas entre date_range_start e date_range_end
e as integrações capturadas entre date_range_start e date_range_end
*/
{% set transacao_table = ref("transacao") %}
{% set transacao_riocard_table = ref("transacao_riocard") %}
{% if execute %}
    {% if is_incremental() %}
        -- Transações Jaé
        {% set partitions_query %}
      SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
      FROM
        -- `{{ transacao_table.database }}.{{ transacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        `rj-smtr.{{ transacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        table_name = "{{ transacao_table.identifier }}"
        AND partition_id != "__NULL__"
        AND DATETIME(last_modified_time, "America/Sao_Paulo") BETWEEN DATETIME("{{var('date_range_start')}}") AND (DATETIME("{{var('date_range_end')}}"))

      UNION DISTINCT

      SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
      FROM
        -- `{{ transacao_riocard_table.database }}.{{ transacao_riocard_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        `rj-smtr.{{ transacao_riocard_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        table_name = "{{ transacao_riocard_table.identifier }}"
        AND partition_id != "__NULL__"
        AND DATETIME(last_modified_time, "America/Sao_Paulo") BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")

        {% endset %}

        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
    {% endif %}
{% endif %}

select
    * except (id_transacao, geo_point_transacao),
    count(id_transacao) as quantidade_passageiros,
    '{{ var("version") }}' as versao
from {{ ref("aux_passageiro_hora") }}
where
    {% if is_incremental() %}
        {% if partition_list | length > 0 %} data in ({{ partition_list | join(", ") }})
        {% else %} data = "2000-01-01"
        {% endif %}
    {% else %} data >= "2023-07-19"
    {% endif %}
group by all
