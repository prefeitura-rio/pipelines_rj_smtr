-- depends_on: {{ ref("ordem_pagamento_servico_operador_dia_invalida") }}
{{
  config(
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": "data_ordem",
      "data_type": "date",
      "granularity": "day"
    },
  )
}}

WITH ordem_pagamento_servico_operador_dia AS (
  SELECT
    data_ordem,
    id_consorcio,
    id_operadora,
    id_ordem_pagamento,
    SUM(quantidade_total_transacao) AS quantidade_total_transacao,
    SUM(valor_total_transacao_liquido) AS valor_total_transacao_liquido,
  FROM
    {{ ref("bilhetagem_servico_operador_dia") }}
  {% if is_incremental() %}
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
  {% endif %}
  GROUP BY
    1,
    2,
    3,
    4
),
ordem_pagamento_consorcio_operador_dia AS (
  SELECT
    data_ordem,
    id_consorcio,
    id_operadora,
    id_ordem_pagamento,
    quantidade_total_transacao,
    valor_total_transacao_liquido_ordem AS valor_total_transacao_liquido
  FROM
    {{ ref("bilhetagem_consorcio_operador_dia") }}
  {% if is_incremental() %}
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
  {% endif %}
),
indicadores AS (
  SELECT
    cod.data_ordem,
    cod.id_consorcio,
    cod.id_operadora,
    cod.id_ordem_pagamento,
    cod.quantidade_total_transacao,
    sod.quantidade_total_transacao AS quantidade_total_transacao_agregacao,
    cod.valor_total_transacao_liquido,
    sod.valor_total_transacao_liquido AS valor_total_transacao_liquido_agregacao,
    ROUND(cod.valor_total_transacao_liquido, 2) != ROUND(sod.valor_total_transacao_liquido, 2) OR cod.quantidade_total_transacao != sod.quantidade_total_transacao AS indicador_agregacao_invalida
  FROM
    ordem_pagamento_consorcio_operador_dia cod
  LEFT JOIN
    ordem_pagamento_servico_operador_dia sod
  USING(
    data_ordem,
    id_consorcio,
    id_operadora,
    id_ordem_pagamento
  )
)
SELECT
  *,
  '{{ var("version") }}' AS versao
FROM
  indicadores
WHERE
  indicador_agregacao_invalida = TRUE