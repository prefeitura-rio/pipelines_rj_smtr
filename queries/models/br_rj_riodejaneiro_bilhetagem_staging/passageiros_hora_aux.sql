{{
  config(materialized="ephemeral")
}}


/*
consulta as partições a serem atualizadas com base nas transações capturadas entre date_range_start e date_range_end
e as integrações capturadas entre date_range_start e date_range_end
*/
{% set transacao_table = ref('transacao') %}
{% if execute %}
  {% if is_incremental() %}

    {% set partitions_query %}
      SELECT
        PARSE_DATE("%Y%m%d", partition_id) AS data
      FROM
        `{{ transacao_table.database }}.{{ transacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        table_name = "{{ transacao_table.identifier }}"
        AND partition_id != "__NULL__"
        AND DATE(last_modified_time, "America/Sao_Paulo") BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
    {% endset %}

    {{ log("Running query: \n"~partitions_query) }}
    {% set partitions = run_query(partitions_query) %}

    {% set partition_list = partitions.columns[0].values() %}
  {% endif %}
{% endif %}

SELECT
  data,
  hora,
  modo,
  consorcio,
  id_servico_jae,
  servico_jae,
  descricao_servico_jae,
  sentido,
  id_transacao,
  tipo_transacao,
  CASE
    WHEN tipo_transacao_smtr = "Gratuidade" THEN tipo_gratuidade
    WHEN tipo_transacao_smtr = "Integração" THEN "Integração"
    WHEN tipo_transacao_smtr = "Transferência" THEN "Transferência"
    ELSE tipo_pagamento
  END AS tipo_transacao_detalhe_smtr,
  tipo_gratuidade,
  tipo_pagamento,
  latitude,
  longitude
FROM
  {{ transacao_table }}
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
  AND id_servico_jae NOT IN ("140", "142")
  AND id_operadora != "2"
  AND (modo = "BRT" OR (modo = "VLT" AND data >= DATE("2024-02-24")))
  AND tipo_transacao IS NOT NULL
