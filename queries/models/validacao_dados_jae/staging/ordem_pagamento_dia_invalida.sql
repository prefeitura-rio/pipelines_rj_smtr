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

WITH ordem_pagamento_consorcio_dia AS (
  SELECT
    data_ordem,
    id_ordem_pagamento,
    SUM(quantidade_total_transacao) AS quantidade_total_transacao,
    SUM(valor_total_transacao_liquido) AS valor_total_transacao_liquido
  FROM
    {{ ref("ordem_pagamento_consorcio_dia") }}
  {% if is_incremental() %}
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
  {% endif %}
  GROUP BY
    1,
    2
),
ordem_pagamento_dia AS (
  SELECT
    data_ordem,
    id_ordem_pagamento,
    quantidade_total_transacao,
    valor_total_transacao_liquido
  FROM
    {{ ref("ordem_pagamento_dia") }}
  {% if is_incremental() %}
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
  {% endif %}
),
indicadores AS (
SELECT
  d.data_ordem,
  d.id_ordem_pagamento,
  d.quantidade_total_transacao,
  cd.quantidade_total_transacao AS quantidade_total_transacao_agregacao,
  d.valor_total_transacao_liquido,
  cd.valor_total_transacao_liquido AS valor_total_transacao_liquido_agregacao,
  ROUND(cd.valor_total_transacao_liquido, 2) != ROUND(d.valor_total_transacao_liquido, 2) OR cd.quantidade_total_transacao != d.quantidade_total_transacao AS indicador_agregacao_invalida
FROM
  ordem_pagamento_dia d
LEFT JOIN
  ordem_pagamento_consorcio_dia cd
USING(data_ordem, id_ordem_pagamento)
)
SELECT
  *,
  '{{ var("version") }}' AS versao
FROM
  indicadores
WHERE
  indicador_agregacao_invalida = TRUE