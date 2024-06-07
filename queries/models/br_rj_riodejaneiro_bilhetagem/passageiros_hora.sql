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

SELECT
  * EXCEPT(id_transacao, latitude, longitude),
  COUNT(id_transacao) AS quantidade_passageiros,
  '{{ var("version") }}' AS versao
FROM
  {{ ref("passageiros_hora_aux") }}
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
