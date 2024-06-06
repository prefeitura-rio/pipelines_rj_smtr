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
  p.* EXCEPT(id_transacao, latitude, longitude),
  geo.tile_id,
  COUNT(id_transacao) AS quantidade_passageiros,
  '{{ var("version") }}' AS versao
FROM
  {{ ref("passageiros_hora_aux") }} p
LEFT JOIN
  {{ source("br_rj_riodejaneiro_geo", "h3_res9") }} geo
ON
  ST_CONTAINS(ST_GEOGFROMTEXT(geo.geometry), ST_GEOGPOINT(p.longitude, p.latitude))
GROUP BY
  data,
  hora,
  modo,
  consorcio,
  id_servico_jae,
  servico_jae,
  descricao_servico_jae,
  sentido,
  tipo_transacao,
  tipo_transacao_detalhe_smtr,
  tipo_gratuidade,
  tipo_pagamento,
  tile_id
