{{ config(materialized="ephemeral") }}

SELECT
  cd_linha,
  vl_tarifa_ida AS tarifa_ida,
  vl_tarifa_volta AS tarifa_volta,
  dt_inicio_validade,
  lead(dt_inicio_validade) OVER (
    PARTITION BY cd_linha ORDER BY nr_sequencia
  ) AS data_fim_validade
FROM {{ ref("staging_linha_tarifa") }}
