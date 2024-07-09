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

{% set transacao_ordem =  ref("aux_transacao_ordem") %}
{% if execute and is_incremental() %}
  {% set transacao_validacao_partition_query %}
    SELECT DISTINCT
      CONCAT("'", DATE(data_transacao), "'") AS data_transacao
    FROM
      transacao_ordem
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
  {% endset %}
  {% set transacao_validacao_partitions = run_query(transacao_validacao_partition_query).columns[0].values() %}
{% endif %}

WITH transacao_invalida AS (
  SELECT
    id_transacao,
    indicador_servico_fora_vigencia
  FROM
    {{ ref("transacao_invalida") }}
  WHERE
    indicador_servico_fora_vigencia = TRUE
  {% if is_incremental() %}
    AND
    {% if transacao_validacao_partitions|length > 0 %}
      data IN ({{ transacao_validacao_partitions|join(', ') }})
    {% else %}
      data = "2000-01-01"
    {% endif %}
  {% endif %}
),
transacao_agg AS (
  SELECT
    t.data_ordem,
    ANY_VALUE(t.id_consorcio) AS id_consorcio,
    t.id_servico_jae,
    t.id_operadora,
    COUNT(*) AS quantidade_total_transacao_captura,
    SUM(t.valor_transacao) AS valor_total_transacao_captura,
    MAX(ti.indicador_servico_fora_vigencia) IS NOT NULL AS indicador_servico_fora_vigencia
  FROM
    {{ ref("aux_transacao_ordem") }} t
  LEFT JOIN
    transacao_invalida ti
  USING(id_transacao)
),
ordem_pagamento AS (
  SELECT
    *
  FROM
    {{ ref("ordem_pagamento_servico_operador_dia") }}
  {% if is_incremental() %}
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
  {% endif %}
),
id_ordem_pagamento AS (
  SELECT
    data_ordem,
    id_ordem_pagamento
  FROM
    {{ ref("ordem_pagamento_dia") }}
  {% if is_incremental() %}
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
  {% endif %}
),
transacao_ordem AS (
  SELECT
    COALESCE(op.data_ordem, t.data_ordem) AS data_ordem,
    COALESCE(op.id_consorcio, t.id_consorcio) AS id_consorcio,
    COALESCE(op.id_operadora, t.id_operadora) AS id_operadora,
    COALESCE(op.id_servico_jae, t.id_servico_jae) AS id_servico_jae,
    op.quantidade_total_transacao,
    op.valor_total_transacao_bruto,
    op.valor_total_transacao_liquido,
    t.quantidade_total_transacao_captura,
    CAST(t.valor_total_transacao_captura + op.valor_rateio_credito + op.valor_rateio_debito AS NUMERIC) AS valor_total_transacao_captura,
    t.indicador_servico_fora_vigencia
  FROM
    ordem_pagamento op
  FULL OUTER JOIN
    transacao_agg t
  USING(data_ordem, id_servico_jae, id_operadora)
)
indicadores AS (
  SELECT
    o.data_ordem,
    id.id_ordem_pagamento,
    o.id_consorcio,
    o.id_operadora,
    o.id_servico_jae,
    o.quantidade_total_transacao,
    o.valor_total_transacao_bruto,
    o.valor_total_transacao_liquido,
    o.quantidade_total_transacao_captura,
    o.valor_total_transacao_captura,
    COALESCE(
      (
        quantidade_total_transacao_captura != quantidade_total_transacao
        OR ROUND(valor_total_transacao_captura, 2) != ROUND(valor_total_transacao_bruto, 2)
      ),
      TRUE
    ) AS indicador_captura_invalida,
    o.indicador_servico_fora_vigencia
  FROM
    transacao_ordem o
  JOIN
    id_ordem_pagamento id
  USING(data_ordem)
)
SELECT
  *,
  '{{ var("version") }}' AS versao
FROM
  indicadores
WHERE
  indicador_servico_fora_vigencia = TRUE
  OR indicador_captura_invalida = TRUE

