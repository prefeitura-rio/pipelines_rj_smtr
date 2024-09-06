{{ config(
    materialized='incremental',
    partition_by={
      "field": "data",
      "data_type": "date",
      "granularity": "day"
    },
    unique_key='data',
    incremental_strategy='insert_overwrite'
) }}

WITH receita_unpivot AS (
  SELECT
    ano,
    CASE
      WHEN mes = 'janeiro' THEN '01'
      WHEN mes = 'fevereiro' THEN '02'
      WHEN mes = 'marco' THEN '03'
      WHEN mes = 'abril' THEN '04'
      WHEN mes = 'maio' THEN '05'
      WHEN mes = 'junho' THEN '06'
      WHEN mes = 'julho' THEN '07'
      WHEN mes = 'agosto' THEN '08'
      WHEN mes = 'setembro' THEN '09'
      WHEN mes = 'outubro' THEN '10'
      WHEN mes = 'novembro' THEN '11'
      WHEN mes = 'dezembro' THEN '12'
    END AS mes,
    SAFE_CAST(REPLACE(REPLACE(valor_arrecadacao, '.', ''), ',', '.') AS NUMERIC) AS valor_arrecadacao
  FROM `rj-smtr-dev.transito_staging.receita_autuacao`
  UNPIVOT (
    valor_arrecadacao FOR mes IN (janeiro, fevereiro, marco, abril, maio, junho, julho, agosto, setembro, outubro, novembro, dezembro)
  )

),

receita_com_data AS (
  SELECT
    PARSE_DATE('%Y-%m-%d', CONCAT(ano, '-', mes, '-01')) AS data,
    ano,
    mes,
    valor_arrecadacao
  FROM receita_unpivot
)

SELECT
  data,
  ano,
  mes,
  valor_arrecadacao
FROM receita_com_data
{% if is_incremental() %}
    WHERE
      data BETWEEN DATE("{{ var('date_range_start') }}") AND DATE("{{ var('date_range_end') }}")
{% endif %}
