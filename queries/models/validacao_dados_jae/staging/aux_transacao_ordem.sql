{{
  config(
    materialized="table",
  )
}}

-- WITH servico_motorista AS (
--     SELECT
--         * EXCEPT(rn)
--     FROM
--     (
--         SELECT
--             id_servico,
--             dt_fechamento,
--             nr_logico_midia,
--             cd_linha,
--             cd_operadora,
--             ROW_NUMBER() OVER (PARTITION BY id_servico, nr_logico_midia ORDER BY timestamp_captura DESC) AS rn
--         FROM
--             {{ ref("staging_servico_motorista") }}
--         {% if is_incremental() %}
--             WHERE
--                 DATE(data) BETWEEN DATE_SUB(DATE("{{var('date_range_start')}}"), INTERVAL 1 DAY) AND DATE("{{var('date_range_end')}}")
--         {% endif %}
--     )
-- ),
WITH transacao AS (
  SELECT
    t.id AS id_transacao,
    t.timestamp_captura,
    DATE(t.data_transacao) AS data_transacao,
    DATE(t.data_processamento) AS data_processamento,
    t.data_processamento AS datetime_processamento,
    t.cd_linha AS id_servico_jae,
    do.id_operadora,
    t.valor_transacao,
    t.tipo_transacao,
    t.id_tipo_modal,
    dc.id_consorcio,
    -- sm.dt_fechamento AS datetime_fechamento_servico,
    -- sm.cd_linha AS cd_linha_servico,
    -- sm.cd_operadora AS cd_operadora_servico,
    t.id_servico
  FROM
    {{ ref("staging_transacao") }} t
  -- LEFT JOIN
  --     servico_motorista sm
  -- ON
  --     sm.id_servico = t.id_servico
  --     AND sm.nr_logico_midia = t.nr_logico_midia_operador
  LEFT JOIN
    {{ ref("operadoras") }} AS do
  ON
    t.cd_operadora = do.id_operadora_jae
  LEFT JOIN
    {{ ref("consorcios") }} AS dc
  ON
    t.cd_consorcio = dc.id_consorcio_jae
  WHERE
    {% if is_incremental() %}
      DATE(t.data) BETWEEN DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 2 DAY) AND DATE("{{var('run_date')}}")
      AND t.data_processamento BETWEEN DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 2 DAY) AND DATE("{{var('run_date')}}")
    {% else %}
      DATE(t.data) <= CURRENT_DATE("America/Sao_Paulo")
      AND DATE(t.data_processamento) <= CURRENT_DATE("America/Sao_Paulo")
    {% endif %}
),
transacao_deduplicada AS (
  SELECT
    t.* EXCEPT(rn),
    DATE_ADD(data_processamento, INTERVAL 1 DAY) AS data_ordem -- TODO: Regra da data por serviços fechados no modo Ônibus quando começar a operação
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_transacao ORDER BY timestamp_captura DESC) AS rn
    FROM
      transacao
  ) t
  WHERE
    rn = 1
)
SELECT
  t.*
FROM
  transacao_deduplicada t
LEFT JOIN
  {{ ref("staging_linha_sem_ressarcimento") }} l
ON
  t.id_servico_jae = l.id_linha
WHERE
  -- Remove dados com data de ordem de pagamento maiores que a execução do modelo
  {% if is_incremental() %}
    t.data_ordem <= DATE("{{var('run_date')}}")
  {% else %}
    t.data_ordem <= CURRENT_DATE("America/Sao_Paulo")
  {% endif %}
  -- Remove linhas de teste que não entram no ressarcimento
  AND l.id_linha IS NULL
  -- Remove gratuidades e transferências da contagem de transações
  AND tipo_transacao NOT IN ('5', '21', '40')