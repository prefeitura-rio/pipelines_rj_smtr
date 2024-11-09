{{
  config(
    partition_by = {
      "field": "feed_start_date",
      "data_type": "date",
      "granularity": "day"
    },
  )
}}

WITH
  dados AS (
  SELECT
    SAFE_CAST(data_versao AS DATE) AS data_versao,
    SAFE_CAST(tipo_os AS STRING) AS tipo_os,
    SAFE_CAST(servico AS STRING) AS servico,
    SAFE_CAST(JSON_VALUE(content, "$.consorcio") AS STRING) AS consorcio,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_03h_e_12h_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_03h_e_12h_dias_uteis, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_12h_e_21h_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_12h_e_21h_dias_uteis, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_00h_e_03h_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_00h_e_03h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_03h_e_06h_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_03h_e_06h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_06h_e_09h_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_06h_e_09h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_09h_e_12h_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_09h_e_12h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_12h_e_15h_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_12h_e_15h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_15h_e_18h_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_15h_e_18h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_18h_e_21h_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_18h_e_21h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_21h_e_24h_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_21h_e_24h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_24h_e_03h_diaseguinte_dias_uteis'), ',', '.') AS STRING) AS partidas_entre_24h_e_03h_diaseguinte_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_03h_e_12h_sabado'), ',', '.') AS STRING) AS partidas_entre_03h_e_12h_sabado, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_12h_e_21h_sabado'), ',', '.') AS STRING) AS partidas_entre_12h_e_21h_sabado, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_00h_e_03h_sabado'), ',', '.') AS STRING) AS partidas_entre_00h_e_03h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_03h_e_06h_sabado'), ',', '.') AS STRING) AS partidas_entre_03h_e_06h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_06h_e_09h_sabado'), ',', '.') AS STRING) AS partidas_entre_06h_e_09h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_09h_e_12h_sabado'), ',', '.') AS STRING) AS partidas_entre_09h_e_12h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_12h_e_15h_sabado'), ',', '.') AS STRING) AS partidas_entre_12h_e_15h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_15h_e_18h_sabado'), ',', '.') AS STRING) AS partidas_entre_15h_e_18h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_18h_e_21h_sabado'), ',', '.') AS STRING) AS partidas_entre_18h_e_21h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_21h_e_24h_sabado'), ',', '.') AS STRING) AS partidas_entre_21h_e_24h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_24h_e_03h_diaseguinte_sabado'), ',', '.') AS STRING) AS partidas_entre_24h_e_03h_diaseguinte_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_03h_e_12h_domingo'), ',', '.') AS STRING) AS partidas_entre_03h_e_12h_domingo, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_12h_e_21h_domingo'), ',', '.') AS STRING) AS partidas_entre_12h_e_21h_domingo, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_00h_e_03h_domingo'), ',', '.') AS STRING) AS partidas_entre_00h_e_03h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_03h_e_06h_domingo'), ',', '.') AS STRING) AS partidas_entre_03h_e_06h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_06h_e_09h_domingo'), ',', '.') AS STRING) AS partidas_entre_06h_e_09h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_09h_e_12h_domingo'), ',', '.') AS STRING) AS partidas_entre_09h_e_12h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_12h_e_15h_domingo'), ',', '.') AS STRING) AS partidas_entre_12h_e_15h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_15h_e_18h_domingo'), ',', '.') AS STRING) AS partidas_entre_15h_e_18h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_18h_e_21h_domingo'), ',', '.') AS STRING) AS partidas_entre_18h_e_21h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_21h_e_24h_domingo'), ',', '.') AS STRING) AS partidas_entre_21h_e_24h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_24h_e_03h_diaseguinte_domingo'), ',', '.') AS STRING) AS partidas_entre_24h_e_03h_diaseguinte_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_03h_e_12h_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_03h_e_12h_ponto_facultativo, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_12h_e_21h_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_12h_e_21h_ponto_facultativo, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_00h_e_03h_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_00h_e_03h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_03h_e_06h_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_03h_e_06h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_06h_e_09h_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_06h_e_09h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_09h_e_12h_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_09h_e_12h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_12h_e_15h_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_12h_e_15h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_15h_e_18h_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_15h_e_18h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_18h_e_21h_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_18h_e_21h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_21h_e_24h_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_21h_e_24h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.partidas_entre_24h_e_03h_diaseguinte_ponto_facultativo'), ',', '.') AS STRING) AS partidas_entre_24h_e_03h_diaseguinte_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_03h_e_12h_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_03h_e_12h_dias_uteis, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_12h_e_21h_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_12h_e_21h_dias_uteis, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_00h_e_03h_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_00h_e_03h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_03h_e_06h_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_03h_e_06h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_06h_e_09h_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_06h_e_09h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_09h_e_12h_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_09h_e_12h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_12h_e_15h_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_12h_e_15h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_15h_e_18h_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_15h_e_18h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_18h_e_21h_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_18h_e_21h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_21h_e_24h_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_21h_e_24h_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_24h_e_03h_diaseguinte_dias_uteis'), ',', '.') AS STRING) AS quilometragem_entre_24h_e_03h_diaseguinte_dias_uteis,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_03h_e_12h_sabado'), ',', '.') AS STRING) AS quilometragem_entre_03h_e_12h_sabado, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_12h_e_21h_sabado'), ',', '.') AS STRING) AS quilometragem_entre_12h_e_21h_sabado, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_00h_e_03h_sabado'), ',', '.') AS STRING) AS quilometragem_entre_00h_e_03h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_03h_e_06h_sabado'), ',', '.') AS STRING) AS quilometragem_entre_03h_e_06h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_06h_e_09h_sabado'), ',', '.') AS STRING) AS quilometragem_entre_06h_e_09h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_09h_e_12h_sabado'), ',', '.') AS STRING) AS quilometragem_entre_09h_e_12h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_12h_e_15h_sabado'), ',', '.') AS STRING) AS quilometragem_entre_12h_e_15h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_15h_e_18h_sabado'), ',', '.') AS STRING) AS quilometragem_entre_15h_e_18h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_18h_e_21h_sabado'), ',', '.') AS STRING) AS quilometragem_entre_18h_e_21h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_21h_e_24h_sabado'), ',', '.') AS STRING) AS quilometragem_entre_21h_e_24h_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_24h_e_03h_diaseguinte_sabado'), ',', '.') AS STRING) AS quilometragem_entre_24h_e_03h_diaseguinte_sabado,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_03h_e_12h_domingo'), ',', '.') AS STRING) AS quilometragem_entre_03h_e_12h_domingo, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_12h_e_21h_domingo'), ',', '.') AS STRING) AS quilometragem_entre_12h_e_21h_domingo, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_00h_e_03h_domingo'), ',', '.') AS STRING) AS quilometragem_entre_00h_e_03h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_03h_e_06h_domingo'), ',', '.') AS STRING) AS quilometragem_entre_03h_e_06h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_06h_e_09h_domingo'), ',', '.') AS STRING) AS quilometragem_entre_06h_e_09h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_09h_e_12h_domingo'), ',', '.') AS STRING) AS quilometragem_entre_09h_e_12h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_12h_e_15h_domingo'), ',', '.') AS STRING) AS quilometragem_entre_12h_e_15h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_15h_e_18h_domingo'), ',', '.') AS STRING) AS quilometragem_entre_15h_e_18h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_18h_e_21h_domingo'), ',', '.') AS STRING) AS quilometragem_entre_18h_e_21h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_21h_e_24h_domingo'), ',', '.') AS STRING) AS quilometragem_entre_21h_e_24h_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_24h_e_03h_diaseguinte_domingo'), ',', '.') AS STRING) AS quilometragem_entre_24h_e_03h_diaseguinte_domingo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_03h_e_12h_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_03h_e_12h_ponto_facultativo, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_12h_e_21h_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_12h_e_21h_ponto_facultativo, -- faixa antiga
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_00h_e_03h_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_00h_e_03h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_03h_e_06h_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_03h_e_06h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_06h_e_09h_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_06h_e_09h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_09h_e_12h_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_09h_e_12h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_12h_e_15h_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_12h_e_15h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_15h_e_18h_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_15h_e_18h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_18h_e_21h_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_18h_e_21h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_21h_e_24h_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_21h_e_24h_ponto_facultativo,
    SAFE_CAST(REPLACE(JSON_VALUE(content, '$.quilometragem_entre_24h_e_03h_diaseguinte_ponto_facultativo'), ',', '.') AS STRING) AS quilometragem_entre_24h_e_03h_diaseguinte_ponto_facultativo
  FROM
    {{ source("br_rj_riodejaneiro_gtfs_staging", "ordem_servico_faixa_horaria") }}
  {# {% if is_incremental() -%} #}
  WHERE
    data_versao = '{{ var("data_versao_gtfs") }}'
  {# {%- endif %} #}
  ),
  dados_agrupados AS (
  SELECT
    data_versao,
    tipo_os,
    servico,
    consorcio,
    CASE
      WHEN column_name LIKE '%dias_uteis%' THEN 'Dia Útil'
      WHEN column_name LIKE '%sabado%' THEN 'Sabado'
      WHEN column_name LIKE '%domingo%' THEN 'Domingo'
      WHEN column_name LIKE '%ponto_facultativo%' THEN 'Ponto Facultativo'
    END AS tipo_dia,
    CASE
      WHEN column_name LIKE '%03h_e_12h%' THEN -- faixa antiga
          '03:00:00'
      WHEN column_name LIKE '%12h_e_21h%' THEN -- faixa antiga
          '12:00:00'
      WHEN column_name LIKE '%00h_e_03h%' THEN
          '00:00:00'
      WHEN column_name LIKE '%03h_e_06h%' THEN
          '03:00:00'
      WHEN column_name LIKE '%06h_e_09h%' THEN
          '06:00:00'
      WHEN column_name LIKE '%09h_e_12h%' THEN
          '09:00:00'
      WHEN column_name LIKE '%12h_e_15h%' THEN
          '12:00:00'
      WHEN column_name LIKE '%15h_e_18h%' THEN
          '15:00:00'
      WHEN column_name LIKE '%18h_e_21h%' THEN
          '18:00:00'
      WHEN column_name LIKE '%21h_e_24h%' THEN
          '21:00:00'
      WHEN column_name LIKE '%24h_e_03h_diaseguinte%' THEN
          '24:00:00'
    END AS faixa_horaria_inicio,
    CASE
      WHEN column_name LIKE '%03h_e_12h%' THEN -- faixa antiga
          '11:59:59'
      WHEN column_name LIKE '%12h_e_21h%' THEN -- faixa antiga
          '20:59:59'
      WHEN column_name LIKE '%00h_e_03h%' THEN
          '02:59:59'
      WHEN column_name LIKE '%03h_e_06h%' THEN
          '05:59:59'
      WHEN column_name LIKE '%06h_e_09h%' THEN
          '08:59:59'
      WHEN column_name LIKE '%09h_e_12h%' THEN
          '11:59:59'
      WHEN column_name LIKE '%12h_e_15h%' THEN
          '14:59:59'
      WHEN column_name LIKE '%15h_e_18h%' THEN
          '17:59:59'
      WHEN column_name LIKE '%18h_e_21h%' THEN
          '20:59:59'
      WHEN column_name LIKE '%21h_e_24h%' THEN
          '23:59:59'
      WHEN column_name LIKE '%24h_e_03h_diaseguinte%' THEN
          '26:59:59'
    END AS faixa_horaria_fim,
    SUM(CASE
        WHEN column_name LIKE '%partidas%' THEN SAFE_CAST(value AS INT64)
        ELSE 0
    END) AS partidas,
    SUM(CASE
        WHEN column_name LIKE '%quilometragem%' THEN SAFE_CAST(value AS FLOAT64)
        ELSE 0
    END) AS quilometragem
  FROM dados
  UNPIVOT (
    value FOR column_name IN (
      partidas_entre_03h_e_12h_dias_uteis, -- faixa antiga
      partidas_entre_12h_e_21h_dias_uteis, -- faixa antiga
      partidas_entre_03h_e_12h_sabado, -- faixa antiga
      partidas_entre_12h_e_21h_sabado, -- faixa antiga
      partidas_entre_03h_e_12h_domingo, -- faixa antiga
      partidas_entre_12h_e_21h_domingo, -- faixa antiga
      partidas_entre_03h_e_12h_ponto_facultativo, -- faixa antiga
      partidas_entre_12h_e_21h_ponto_facultativo, -- faixa antiga
      quilometragem_entre_03h_e_12h_dias_uteis, -- faixa antiga
      quilometragem_entre_12h_e_21h_dias_uteis, -- faixa antiga
      quilometragem_entre_03h_e_12h_sabado, -- faixa antiga
      quilometragem_entre_12h_e_21h_sabado, -- faixa antiga
      quilometragem_entre_03h_e_12h_domingo, -- faixa antiga
      quilometragem_entre_12h_e_21h_domingo, -- faixa antiga
      quilometragem_entre_03h_e_12h_ponto_facultativo, -- faixa antiga
      quilometragem_entre_12h_e_21h_ponto_facultativo, -- faixa antiga
      partidas_entre_00h_e_03h_dias_uteis,
      quilometragem_entre_00h_e_03h_dias_uteis,
      partidas_entre_03h_e_06h_dias_uteis,
      quilometragem_entre_03h_e_06h_dias_uteis,
      partidas_entre_06h_e_09h_dias_uteis,
      quilometragem_entre_06h_e_09h_dias_uteis,
      partidas_entre_09h_e_12h_dias_uteis,
      quilometragem_entre_09h_e_12h_dias_uteis,
      partidas_entre_12h_e_15h_dias_uteis,
      quilometragem_entre_12h_e_15h_dias_uteis,
      partidas_entre_15h_e_18h_dias_uteis,
      quilometragem_entre_15h_e_18h_dias_uteis,
      partidas_entre_18h_e_21h_dias_uteis,
      quilometragem_entre_18h_e_21h_dias_uteis,
      partidas_entre_21h_e_24h_dias_uteis,
      quilometragem_entre_21h_e_24h_dias_uteis,
      partidas_entre_24h_e_03h_diaseguinte_dias_uteis,
      quilometragem_entre_24h_e_03h_diaseguinte_dias_uteis,
      partidas_entre_00h_e_03h_sabado,
      quilometragem_entre_00h_e_03h_sabado,
      partidas_entre_03h_e_06h_sabado,
      quilometragem_entre_03h_e_06h_sabado,
      partidas_entre_06h_e_09h_sabado,
      quilometragem_entre_06h_e_09h_sabado,
      partidas_entre_09h_e_12h_sabado,
      quilometragem_entre_09h_e_12h_sabado,
      partidas_entre_12h_e_15h_sabado,
      quilometragem_entre_12h_e_15h_sabado,
      partidas_entre_15h_e_18h_sabado,
      quilometragem_entre_15h_e_18h_sabado,
      partidas_entre_18h_e_21h_sabado,
      quilometragem_entre_18h_e_21h_sabado,
      partidas_entre_21h_e_24h_sabado,
      quilometragem_entre_21h_e_24h_sabado,
      partidas_entre_24h_e_03h_diaseguinte_sabado,
      quilometragem_entre_24h_e_03h_diaseguinte_sabado,
      partidas_entre_00h_e_03h_domingo,
      quilometragem_entre_00h_e_03h_domingo,
      partidas_entre_03h_e_06h_domingo,
      quilometragem_entre_03h_e_06h_domingo,
      partidas_entre_06h_e_09h_domingo,
      quilometragem_entre_06h_e_09h_domingo,
      partidas_entre_09h_e_12h_domingo,
      quilometragem_entre_09h_e_12h_domingo,
      partidas_entre_12h_e_15h_domingo,
      quilometragem_entre_12h_e_15h_domingo,
      partidas_entre_15h_e_18h_domingo,
      quilometragem_entre_15h_e_18h_domingo,
      partidas_entre_18h_e_21h_domingo,
      quilometragem_entre_18h_e_21h_domingo,
      partidas_entre_21h_e_24h_domingo,
      quilometragem_entre_21h_e_24h_domingo,
      partidas_entre_24h_e_03h_diaseguinte_domingo,
      quilometragem_entre_24h_e_03h_diaseguinte_domingo,
      partidas_entre_00h_e_03h_ponto_facultativo,
      quilometragem_entre_00h_e_03h_ponto_facultativo,
      partidas_entre_03h_e_06h_ponto_facultativo,
      quilometragem_entre_03h_e_06h_ponto_facultativo,
      partidas_entre_06h_e_09h_ponto_facultativo,
      quilometragem_entre_06h_e_09h_ponto_facultativo,
      partidas_entre_09h_e_12h_ponto_facultativo,
      quilometragem_entre_09h_e_12h_ponto_facultativo,
      partidas_entre_12h_e_15h_ponto_facultativo,
      quilometragem_entre_12h_e_15h_ponto_facultativo,
      partidas_entre_15h_e_18h_ponto_facultativo,
      quilometragem_entre_15h_e_18h_ponto_facultativo,
      partidas_entre_18h_e_21h_ponto_facultativo,
      quilometragem_entre_18h_e_21h_ponto_facultativo,
      partidas_entre_21h_e_24h_ponto_facultativo,
      quilometragem_entre_21h_e_24h_ponto_facultativo,
      partidas_entre_24h_e_03h_diaseguinte_ponto_facultativo,
      quilometragem_entre_24h_e_03h_diaseguinte_ponto_facultativo
    )
  )
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT
  fi.feed_version,
  fi.feed_start_date,
  fi.feed_end_date,
  d.* EXCEPT(data_versao),
  '{{ var("version") }}' AS versao_modelo
FROM
  dados_agrupados AS d
LEFT JOIN
  {{ ref('feed_info_gtfs') }} AS fi
ON
  d.data_versao = fi.feed_start_date
{# {% if is_incremental() -%} #}
WHERE
  d.data_versao = '{{ var("data_versao_gtfs") }}'
  AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{# {% else %}
WHERE
  d.data_versao >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
{%- endif %} #}
