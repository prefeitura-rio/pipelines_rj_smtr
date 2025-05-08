{% if var("encontro_contas_modo") == "" %}
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
  consorcio_rdo,
  servico_tratado_rdo,
  linha_rdo,
  tipo_servico_rdo,
  ordem_servico_rdo,
  COUNT(data_rdo) AS quantidade_dias_rdo,
  SUM(receita_tarifaria_aferida_rdo) AS receita_tarifaria_aferida_rdo
FROM
  quinzenas qz
LEFT JOIN (
  SELECT * from {{ ref("aux_balanco_rdo_servico_dia") }} WHERE servico is null
) bs
ON
  bs.data_rdo BETWEEN qz.data_inicial_quinzena
  AND qz.data_final_quinzena
GROUP BY
  1,2,3,4,5,6,7,8
ORDER BY
  2,4,5,6,7,8
{% else %}
{{ config(enabled=false) }}
{% endif %}