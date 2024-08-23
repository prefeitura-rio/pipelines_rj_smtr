{{
  config(
    materialized='table',
  )
}}

WITH dados_agrupados AS (
  SELECT
    servico,
    CASE
      WHEN column_name LIKE '%dias_uteis%' THEN 'Dia Útil'
      WHEN column_name LIKE '%sabado%' THEN 'Sabado'
      ELSE 'Domingo'
    END AS tipo_dia,
    CASE
      WHEN column_name LIKE '%00h_e_03h%' THEN
          '00:00:00'
      WHEN column_name LIKE '%03h_e_12h%' THEN
          '03:00:00'
      WHEN column_name LIKE '%12h_e_21h%' THEN
          '12:00:00'
      WHEN column_name LIKE '%21h_e_24h%' THEN
          '21:00:00'
      WHEN column_name LIKE '%24h_e_03h_diaseguinte%' THEN
          '24:00:00'
    END AS faixa_horaria_inicio,
    CASE
      WHEN column_name LIKE '%00h_e_03h%' THEN
          '02:59:59'
      WHEN column_name LIKE '%03h_e_12h%' THEN
          '11:59:59'
      WHEN column_name LIKE '%12h_e_21h%' THEN
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
  FROM rj-smtr-dev.projeto_subsidio_sppo.20240813_partidas
  UNPIVOT (
    value FOR column_name IN (
      partidas_entre_00h_e_03h_dias_uteis,
      quilometragem_entre_00h_e_03h_dias_uteis,
      partidas_entre_03h_e_12h_dias_uteis,
      quilometragem_entre_03h_e_12h_dias_uteis,
      partidas_entre_12h_e_21h_dias_uteis,
      quilometragem_entre_12h_e_21h_dias_uteis,
      partidas_entre_21h_e_24h_dias_uteis,
      quilometragem_entre_21h_e_24h_dias_uteis,
      partidas_entre_24h_e_03h_diaseguinte_dias_uteis,
      quilometragem_entre_24h_e_03h_diaseguinte_dias_uteis,
      partidas_entre_03h_e_12h_sabado,
      quilometragem_entre_03h_e_12h_sabado,
      partidas_entre_12h_e_21h_sabado,
      quilometragem_entre_12h_e_21h_sabado,
      partidas_entre_21h_e_24h_sabado,
      quilometragem_entre_21h_e_24h_sabado,
      partidas_entre_24h_e_03h_diaseguinte_sabado,
      quilometragem_entre_24h_e_03h_diaseguinte_sabado,
      partidas_entre_03h_e_12h_domingo,
      quilometragem_entre_03h_e_12h_domingo,
      partidas_entre_12h_e_21h_domingo,
      quilometragem_entre_12h_e_21h_domingo,
      partidas_entre_21h_e_24h_domingo,
      quilometragem_entre_21h_e_24h_domingo,
      partidas_entre_24h_e_03h_diaseguinte_domingo,
      quilometragem_entre_24h_e_03h_diaseguinte_domingo
    )
  )
  GROUP BY 1, 2, 3, 4
)

SELECT * FROM dados_agrupados
