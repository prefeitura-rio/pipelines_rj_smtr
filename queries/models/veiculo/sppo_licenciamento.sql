{{
  config(
    materialized="ephemeral"
  )
}}

SELECT
  *
FROM
  {{ ref("licenciamento") }}
WHERE
  tipo_veiculo NOT LIKE "%ROD%"