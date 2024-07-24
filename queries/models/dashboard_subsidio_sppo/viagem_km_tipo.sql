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
-- 3. Parâmetros de subsídio
  subsidio_parametros AS (
  SELECT
    DISTINCT data_inicio,
    data_fim,
    status,
    subsidio_km
  FROM
    {{ ref("subsidio_valor_km_tipo_viagem") }} ),
-- 4. Viagem com tolerância de 30 minutos, limitada pela viagem anterior
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
-- 5. Contagem de transações
transacao_contagem AS (
  SELECT
    v.data,
    v.id_viagem,
    COUNT(t.datetime_transacao) AS transacoes_contagem
  FROM
    {{ ref("transacao") }} AS t
  JOIN
    viagem_com_tolerancia AS v
  ON
    t.id_veiculo = SUBSTR(v.id_veiculo, 2)
    AND t.data = v.data
    AND t.datetime_transacao BETWEEN v.datetime_partida_com_tolerancia AND v.datetime_chegada
  GROUP BY
    v.data, v.id_viagem
),
-- 6. Contagem de transações Riocard
transacao_riocard_contagem AS (
  SELECT
    v.data,
    v.id_viagem,
    COUNT(tr.datetime_transacao) AS transacoes_riocard_contagem
  FROM
    {{ ref("transacao_riocard") }} AS tr
  JOIN
    viagem_com_tolerancia AS v
  ON
    tr.id_veiculo = SUBSTR(v.id_veiculo, 2)
    AND tr.data = v.data
    AND tr.datetime_transacao BETWEEN v.datetime_partida_com_tolerancia AND v.datetime_chegada
  GROUP BY
    v.data, v.id_viagem
)
-- 7. Verificação de estado do equipamento
estado_equipamento_verificacao AS (
  SELECT
    v.data,
    v.id_viagem,
    COUNT(CASE WHEN g.estado_equipamento != "ABERTO" THEN 1 END) AS conta_estado_fechado
  FROM
    {{ ref("gps_validador") }} AS g
  JOIN
    viagem_com_tolerancia AS v
  ON
    g.id_veiculo = SUBSTR(v.id_veiculo, 2)
    AND g.data = v.data
    AND g.datetime_gps BETWEEN v.datetime_partida AND v.datetime_chegada
  GROUP BY
    v.data, v.id_viagem
)
SELECT
  v.data,
  v.servico,
  CASE
    WHEN v.data >= "2024-07-20"
      AND ((COALESCE(tc.transacoes_contagem, 0) = 0
        AND COALESCE(trc.transacoes_riocard_contagem, 0) = 0)
        OR eev.conta_estado_fechado != 0)
      AND ve.status IN ("Licenciado com ar e não autuado", "Licenciado sem ar e não autuado")
      THEN "Sem transação"
    ELSE ve.status
  END AS tipo_viagem,
  v.id_viagem,
  v.distancia_planejada,
  t.subsidio_km
FROM
  viagem_com_tolerancia AS v
LEFT JOIN
  veiculos AS ve
USING
  (data, id_veiculo)
LEFT JOIN
  subsidio_parametros AS t
ON
  v.data BETWEEN t.data_inicio AND t.data_fim
  AND ve.status = t.status
LEFT JOIN
  transacao_contagem AS tc
ON
  v.data = tc.data
  AND v.id_viagem = tc.id_viagem
LEFT JOIN
  transacao_riocard_contagem AS trc
ON
  v.data = trc.data
  AND v.id_viagem = trc.id_viagem
LEFT JOIN
  estado_equipamento_verificacao AS eev
ON
  v.data = eev.data
  AND v.id_viagem = eev.id_viagem