{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

WITH
-- 1. Viagens realizadas
  viagem AS (
  SELECT
    data,
    servico_realizado AS servico,
    datetime_partida,
    datetime_chegada,
    id_veiculo,
    id_viagem,
    distancia_planejada
 FROM
    {{ ref("viagem_completa") }}
    -- rj-smtr.projeto_subsidio_sppo.viagem_completa
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" ) ),
-- 2. Status dos veículos
  veiculos AS (
  SELECT
    data,
    id_veiculo,
    status
  FROM
    {{ ref("sppo_veiculo_dia") }}
    -- rj-smtr.veiculo.sppo_veiculo_dia
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}") ),
-- 3. Viagem com tolerância de 30 minutos, limitada pela viagem anterior
viagem_com_tolerancia AS (
  SELECT
    v.*,
    LAG(v.datetime_chegada) OVER (PARTITION BY v.id_veiculo ORDER BY v.datetime_partida) AS viagem_anterior_chegada,
    CASE
      WHEN LAG(v.datetime_chegada) OVER (PARTITION BY v.id_veiculo ORDER BY v.datetime_partida) IS NULL THEN
        DATETIME(TIMESTAMP_SUB(datetime_partida, INTERVAL 30 MINUTE))
      ELSE
        GREATEST(
          DATETIME(TIMESTAMP_SUB(datetime_partida, INTERVAL 30 MINUTE)),
          LAG(v.datetime_chegada) OVER (PARTITION BY v.id_veiculo ORDER BY v.datetime_partida)
        )
    END AS datetime_partida_com_tolerancia
  FROM
    viagem AS v
),
-- 4. Contagem de transações
transacao_contagem AS (
  SELECT
    v.data,
    v.id_viagem,
    COUNT(t.datetime_transacao) AS quantidade_transacao
  FROM
    {{ ref("transacao") }} AS t
    -- rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao AS t
  JOIN
    viagem_com_tolerancia AS v
  ON
    t.id_veiculo = SUBSTR(v.id_veiculo, 2)
    AND t.data = v.data
    AND t.datetime_transacao BETWEEN v.datetime_partida_com_tolerancia AND v.datetime_chegada
  GROUP BY
    v.data, v.id_viagem
),
-- 5. Contagem de transações Riocard
transacao_riocard_contagem AS (
  SELECT
    v.data,
    v.id_viagem,
    COUNT(tr.datetime_transacao) AS quantidade_transacao_riocard
  FROM
    {{ ref("transacao_riocard") }} AS tr
    -- rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao_riocard AS tr
  JOIN
    viagem_com_tolerancia AS v
  ON
    tr.id_veiculo = SUBSTR(v.id_veiculo, 2)
    AND tr.data = v.data
    AND tr.datetime_transacao BETWEEN v.datetime_partida_com_tolerancia AND v.datetime_chegada
  GROUP BY
    v.data, v.id_viagem
),
-- transacao_riocard_contagem AS (
--   SELECT
--     v.data,
--     v.id_viagem,
--     COUNT(tr.data_transacao) AS quantidade_transacao_riocard
--   FROM
--     -- {{ ref("transacao_riocard") }} AS tr
--     rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.transacao_riocard AS tr
--   JOIN
--     viagem_com_tolerancia AS v
--   ON
--     -- tr.id_veiculo = SUBSTR(v.id_veiculo, 2)
--     tr.veiculo_id = SUBSTR(v.id_veiculo, 2)
--     AND EXTRACT(DATE FROM tr.data_transacao)  = v.data
--     AND tr.data_transacao BETWEEN v.datetime_partida_com_tolerancia AND v.datetime_chegada
--   GROUP BY
--     v.data, v.id_viagem
-- ),
-- 6. Verificação de estado do equipamento
estado_equipamento_verificacao AS (
  SELECT
    v.data,
    v.id_viagem,
    IF(COUNT(CASE WHEN g.estado_equipamento != "ABERTO" THEN 1 END) = 0, TRUE, FALSE) AS indicador_estado_equipamento_aberto
  FROM
    {{ ref("gps_validador") }} AS g
    -- rj-smtr.br_rj_riodejaneiro_bilhetagem.gps_validador AS g
  JOIN
    viagem AS v
  ON
    g.id_veiculo = SUBSTR(v.id_veiculo, 2)
    AND g.data = v.data
    AND g.datetime_gps BETWEEN v.datetime_partida AND v.datetime_chegada
  GROUP BY
    v.data, v.id_viagem
)
SELECT
  v.data,
  v.id_viagem,
  v.id_veiculo,
  v.servico,
  CASE
    WHEN v.data >= DATE("{{ var("DATA_SUBSIDIO_V8_INICIO") }}")
    --   AND ((COALESCE(t.quantidade_transacao, 0) = 0
    --     AND COALESCE(tr.quantidade_transacao_riocard, 0) = 0)
    --     OR eev.conta_estado_fechado != 0)
      AND (COALESCE(tr.quantidade_transacao_riocard, 0) = 0
        OR COALESCE(eev.indicador_estado_equipamento_aberto, FALSE) = FALSE)
      AND ve.status IN ("Licenciado com ar e não autuado", "Licenciado sem ar e não autuado")
      THEN "Sem transação"
    ELSE ve.status
  END AS tipo_viagem,
  v.distancia_planejada,
  COALESCE(t.quantidade_transacao, 0) AS quantidade_transacao,
  COALESCE(tr.quantidade_transacao_riocard, 0) AS quantidade_transacao_riocard,
  COALESCE(eev.indicador_estado_equipamento_aberto, FALSE) AS indicador_estado_equipamento_aberto,
  v.datetime_partida_com_tolerancia AS datetime_partida_bilhetagem,
  v.datetime_partida,
  v.datetime_chegada,
  CURRENT_DATETIME("America/Sao_Paulo") AS datetime_ultima_atualizacao
FROM
  viagem_com_tolerancia AS v
LEFT JOIN
  veiculos AS ve
USING
  (data, id_veiculo)
LEFT JOIN
  transacao_contagem AS t
USING
  (data, id_viagem)
LEFT JOIN
  transacao_riocard_contagem AS tr
USING
  (data, id_viagem)
LEFT JOIN
  estado_equipamento_verificacao AS eev
USING
  (data, id_viagem)