{{
  config(
    materialized="ephemeral"
  )
}}

SELECT
  *
FROM
  {# {{ ref("licenciamento") }} #}
`rj-smtr.veiculo.licenciamento`
WHERE
  tipo_veiculo NOT LIKE "%ROD%"
  and modo = 'ONIBUS'