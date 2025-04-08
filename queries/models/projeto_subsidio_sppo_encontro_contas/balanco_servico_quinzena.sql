{{ config(alias=this.name ~ var('encontro_contas_modo')) }}

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
  sum(km_subsidiada) as km_subsidiada,
  {# sum(receita_total_esperada) as receita_total_esperada, #}
  sum(receita_tarifaria_esperada) as receita_tarifaria_esperada,
  {# sum(subsidio_esperado) as subsidio_esperado, #}
  {# sum(subsidio_glosado) as subsidio_glosado, #}
  {# sum(receita_total_aferida) as receita_total_aferida, #}
  sum(receita_tarifaria_aferida) as receita_tarifaria_aferida,
  {# sum(subsidio_pago) as subsidio_pago, #}
  sum(saldo) as saldo
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