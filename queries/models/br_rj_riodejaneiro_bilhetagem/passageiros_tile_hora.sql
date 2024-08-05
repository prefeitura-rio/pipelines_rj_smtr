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
  p.* EXCEPT(id_transacao, geo_point_transacao),
  geo.tile_id,
  COUNT(id_transacao) AS quantidade_passageiros,
  '{{ var("version") }}' AS versao
FROM
  {{ ref("aux_passageiros_hora") }} p
JOIN
  {{ ref("aux_h3_res9") }} geo
ON
  ST_CONTAINS(geo.geometry, geo_point_transacao)
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
  tipo_pagamento,
  tile_id
