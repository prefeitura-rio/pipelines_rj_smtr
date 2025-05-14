{% if var("start_date") >= var("DATA_SUBSIDIO_V9_INICIO") %}
{{ config(enabled=false) }}
{% else %}
{{ config(alias=this.name ~ var('encontro_contas_modo')) }}
{% endif %}

WITH
  datas AS (
  SELECT
    DATA,
    CASE
      WHEN EXTRACT(DAY FROM DATA) <= 15 THEN FORMAT_DATE('%Y-%m-Q1', DATA)
      ELSE FORMAT_DATE('%Y-%m-Q2', DATA)
  END
    AS quinzena,
  FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE("{{ var('start_date') }}"), DATE("{{ var('end_date') }}"), INTERVAL 1 DAY)) AS DATA ),
  quinzenas AS (
  SELECT
    quinzena,
    MIN(DATA) AS data_inicial_quinzena,
    MAX(DATA) AS data_final_quinzena
  FROM
    datas
  GROUP BY
    quinzena )
SELECT
  quinzena,
  data_inicial_quinzena,
  data_final_quinzena,
  consorcio,
  servico,
  COUNT(DATA) AS quantidade_dias_subsidiado,
  SUM(km_subsidiada) AS km_subsidiada,
  SUM(receita_total_esperada) AS receita_total_esperada,
  SUM(receita_tarifaria_esperada) AS receita_tarifaria_esperada,
  SUM(subsidio_esperado) AS subsidio_esperado,
  SUM(subsidio_glosado) AS subsidio_glosado,
  SUM(receita_total_aferida) AS receita_total_aferida,
  SUM(receita_tarifaria_aferida) AS receita_tarifaria_aferida,
  SUM(subsidio_pago) AS subsidio_pago,
  SUM(saldo) AS saldo
FROM
  quinzenas qz
LEFT JOIN
  {{ ref("balanco_servico_dia" ~ var('encontro_contas_modo')) }} bs
ON
  bs.data BETWEEN qz.data_inicial_quinzena
  AND qz.data_final_quinzena
GROUP BY
  quinzena,
  data_inicial_quinzena,
  data_final_quinzena,
  consorcio,
  servico
ORDER BY
  data_inicial_quinzena,
  consorcio,
  servico