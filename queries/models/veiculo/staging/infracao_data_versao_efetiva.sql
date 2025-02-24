{{ config(
        materialized="table")}}

WITH infracao_date AS (
  SELECT DISTINCT SAFE_CAST(data AS DATE) AS data_infracao
  FROM {{ ref('infracao_staging') }}
),
periodo AS (
  SELECT DATE_ADD(DATE '2023-02-10', INTERVAL n DAY) AS data
  FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE '2026-01-01', DATE '2023-02-10', DAY))) AS n
),
data_versao_calc AS (
  SELECT
    periodo.data,
    (
      SELECT MIN(data_infracao)
      FROM infracao_date
      WHERE data_infracao >= DATE_ADD(periodo.data, INTERVAL 7 DAY)
    ) AS data_versao
  FROM periodo
)
SELECT *
FROM data_versao_calc