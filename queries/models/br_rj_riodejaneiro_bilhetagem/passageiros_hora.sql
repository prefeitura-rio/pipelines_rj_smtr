{{
  config(
    materialized='incremental',
    partition_by={
      "field":"data",
      "data_type": "date",
      "granularity":"day"
    },
    incremental_strategy="insert_overwrite"
  )
}}

/*
consulta as partições a serem atualizadas com base nas transações capturadas entre date_range_start e date_range_end
e as integrações capturadas entre date_range_start e date_range_end
*/
{% set transacao_table = ref('transacao') %}
{% set transacao_riocard_table = ref('transacao_riocard') %}
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

SELECT
  * EXCEPT(id_transacao, geo_point_transacao),
  COUNT(id_transacao) AS quantidade_passageiros,
  '{{ var("version") }}' AS versao
FROM
  {{ ref("aux_passageiros_hora") }}
WHERE
{% if is_incremental() %}
  {% if partition_list|length > 0 %}
    data IN ({{ partition_list|join(', ') }})
  {% else %}
    data = "2000-01-01"
  {% endif %}
{% else %}
  data >= "2023-07-19"
{% endif %}
GROUP BY
  data,
  hora,
  modo,
  consorcio,
  id_servico_jae,
  servico_jae,
  descricao_servico_jae,
  sentido,
  tipo_transacao_smtr,
  tipo_transacao_detalhe_smtr,
  tipo_gratuidade,
  tipo_pagamento
