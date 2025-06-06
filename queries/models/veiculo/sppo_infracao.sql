{{
  config(
    materialized="ephemeral"
  )
}}

SELECT
  *
FROM
  {{ ref("infracao") }}
WHERE
  modo = 'ONIBUS'
  AND placa IS NOT NULL