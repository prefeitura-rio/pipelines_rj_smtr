{{
  config(
    materialized="ephemeral"
  )
}}

SELECT
  *
FROM
  {{ ref("infracao") }}
  {# `rj-smtr.veiculo.infracao` #}
WHERE
  modo = 'ONIBUS'
  AND placa IS NOT NULL