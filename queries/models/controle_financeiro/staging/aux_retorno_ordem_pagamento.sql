{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data_ordem",
      "data_type":"date",
      "granularity": "day"
    },
    unique_key="unique_id"
  )
}}

SELECT
  dataOrdem AS data_ordem,
  DATE(dataVencimento) AS data_pagamento,
  idConsorcio AS id_consorcio,
  idOperadora AS id_operadora,
  CONCAT(dataOrdem, idConsorcio, idOperadora) AS unique_id,
  valorRealEfetivado AS valor_pago
FROM
  {{ ref("staging_arquivo_retorno") }}
WHERE
  isPago = TRUE
{% if is_incremental() %}
  AND DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
{% endif %}