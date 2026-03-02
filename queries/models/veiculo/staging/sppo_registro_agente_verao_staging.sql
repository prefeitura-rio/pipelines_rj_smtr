{{ config(materialized="view", alias="sppo_registro_agente_verao") }}

SELECT
  safe_cast(parse_datetime("%d/%m/%Y %H:%M:%S", datetime_registro) AS date)
    AS data,
  safe_cast(
    parse_datetime("%d/%m/%Y %H:%M:%S", datetime_registro) AS datetime
  ) AS datetime_registro,
  -- fmt: off
  sha256(
    parse_datetime("%d/%m/%Y %H:%M:%S", datetime_registro)
    || "_"
    || safe_cast(email AS string)
  ) AS id_registro,
  -- fmt: on
  safe_cast(json_value(content, '$.id_veiculo') AS string) AS id_veiculo,
  safe_cast(json_value(content, '$.servico') AS string) AS servico,
  safe_cast(json_value(content, '$.link_foto') AS string) AS link_foto,
  safe_cast(json_value(content, '$.validacao') AS bool) AS validacao,
  date(data) AS data_captura,
  safe_cast(
    datetime(
      timestamp_trunc(timestamp(timestamp_captura), second), "America/Sao_Paulo"
    ) AS datetime
  ) AS datetime_captura,
  "{{ var('version') }}" AS versao
FROM {{ source("veiculo_staging", "sppo_registro_agente_verao") }}
