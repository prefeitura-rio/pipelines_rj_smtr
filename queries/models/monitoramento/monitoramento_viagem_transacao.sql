{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        alias='viagem_transacao',
    )
}}

WITH
  -- 1. Transações Jaé
  transacao AS (
    SELECT
      id_veiculo,
      datetime_transacao
    FROM
      -- {{ ref("transacao") }}
      rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao
    WHERE
    data >= DATE("2024-10-01")
    {% if is_incremental() %}
      AND data BETWEEN DATE("{{ var("start_date") }}")
      AND DATE_ADD(DATE("{{ var("end_date") }}"), INTERVAL 1 DAY)
    {% endif %}
  ),
  -- 2. Transações RioCard
  transacao_riocard AS (
    SELECT
      id_veiculo,
      datetime_transacao
    FROM
      -- {{ ref("transacao_riocard") }}
      rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao_riocard
    WHERE
      data >= DATE("2024-10-01")
      {% if is_incremental() %}
        AND data BETWEEN DATE("{{ var("start_date") }}")
        AND DATE_ADD(DATE("{{ var("end_date") }}"), INTERVAL 1 DAY)
      {% endif %}
  ),
  -- 3. GPS Validador
  gps_validador AS (
    SELECT
      data,
      datetime_gps,
      id_veiculo,
      id_validador,
      estado_equipamento,
      latitude,
      longitude
    FROM
      -- {{ ref("gps_validador") }}
      rj-smtr.br_rj_riodejaneiro_bilhetagem.gps_validador
    WHERE
      data >= DATE("2024-10-01")
      {% if is_incremental() %}
        AND data BETWEEN DATE("{{ var("start_date") }}")
        AND DATE_ADD(DATE("{{ var("end_date") }}"), INTERVAL 1 DAY)
      {% endif %}
      AND (latitude != 0 OR longitude != 0)
  ),
  -- 4. Viagens realizadas
  viagem AS (
  SELECT
    data,
    servico_realizado AS servico,
    datetime_partida,
    datetime_chegada,
    id_veiculo,
    id_viagem,
    sentido,
    distancia_planejada
 FROM
    -- {{ ref("viagem_completa") }}
    rj-smtr.projeto_subsidio_sppo.viagem_completa
  WHERE
    data >= DATE("2024-10-01")
    {% if is_incremental() %}
      AND data BETWEEN DATE("{{ var("start_date") }}")
      AND DATE( "{{ var("end_date") }}" )
    {% endif %}
  ),
  -- 5. Status dos veículos
  veiculos AS (
  SELECT
    data,
    id_veiculo,
    status
  FROM
    -- {{ ref("sppo_veiculo_dia") }}
    rj-smtr.veiculo.sppo_veiculo_dia
  WHERE
    data >= DATE("2024-10-01")
    {% if is_incremental() %}
      AND data BETWEEN DATE("{{ var("start_date") }}")
      AND DATE( "{{ var("end_date") }}" )
    {% endif %}
  ),
  -- 6. Viagem, para fins de contagem de passageiros, com tolerância de 30 minutos apenas para a primeira viagem
  -- Obtem o Datetime de chegada da viagem anterior
  chegada_anterior AS (
    SELECT
      v.*,
      LAG(v.datetime_chegada) OVER (PARTITION BY v.id_veiculo ORDER BY v.datetime_partida) AS viagem_anterior_chegada,
    FROM
      viagem AS v
  ),
  -- Adiciona tolerancia de 30 minutos a primeira viagem ou até o fim da viagem anterior caso exista
  viagem_com_tolerancia AS (
    SELECT
      v.*,
      CASE
        WHEN viagem_anterior_chegada IS NULL THEN
          DATETIME(TIMESTAMP_SUB(datetime_partida, INTERVAL 30 MINUTE))
        ELSE DATETIME(TIMESTAMP_ADD(viagem_anterior_chegada, INTERVAL 1 SECOND))
      END AS datetime_partida_com_tolerancia
    FROM
      chegada_anterior AS v
  ),
  -- 7. Contagem de transações Jaé
  transacao_contagem AS (
    SELECT
      v.data,
      v.id_viagem,
      COUNT(t.datetime_transacao) AS quantidade_transacao
    FROM
      transacao AS t
    JOIN
      viagem_com_tolerancia AS v
    ON
      t.id_veiculo = SUBSTR(v.id_veiculo, 2)
      AND t.datetime_transacao BETWEEN v.datetime_partida_com_tolerancia AND v.datetime_chegada
    GROUP BY
      v.data, v.id_viagem
  ),
  -- 5. Contagem de transações RioCard
  transacao_riocard_contagem AS (
    SELECT
      v.data,
      v.id_viagem,
      COUNT(tr.datetime_transacao) AS quantidade_transacao_riocard
    FROM
      transacao_riocard AS tr
    JOIN
      viagem_com_tolerancia AS v
    ON
      tr.id_veiculo = SUBSTR(v.id_veiculo, 2)
      AND tr.datetime_transacao BETWEEN v.datetime_partida_com_tolerancia AND v.datetime_chegada
    GROUP BY
      v.data, v.id_viagem
  ),
  -- 6. Ajusta estado do equipamento
  -- Agrupa mesma posição para mesmo validador e veículo, mantendo preferencialmente o estado do equipamento "ABERTO"
  estado_equipamento_aux AS (
    SELECT
      data,
      id_validador,
      id_veiculo,
      latitude,
      longitude,
      IF(COUNT(CASE WHEN estado_equipamento = "ABERTO" THEN 1 END) >= 1, "ABERTO", "FECHADO") AS estado_equipamento,
      MIN(datetime_gps) AS datetime_gps,
    FROM
      gps_validador
    GROUP BY
      1,
      2,
      3,
      4,
      5
  ),
  -- 7. Relacionamento entre estado do equipamento e viagem
  gps_validador_viagem AS (
    SELECT
      v.data,
      e.datetime_gps,
      v.id_viagem,
      e.id_validador,
      e.estado_equipamento,
      e.latitude,
      e.longitude
    FROM
      estado_equipamento_aux AS e
    JOIN
      viagem AS v
    ON
      e.id_veiculo = SUBSTR(v.id_veiculo, 2)
      AND e.datetime_gps BETWEEN v.datetime_partida AND v.datetime_chegada
  ),
  -- 8. Calcula a porcentagem de estado do equipamento "ABERTO" por validador e viagem
  estado_equipamento_perc AS (
    SELECT
      data,
      id_viagem,
      id_validador,
      COUNTIF(estado_equipamento = "ABERTO") / COUNT(*) AS percentual_estado_equipamento_aberto
    FROM
      gps_validador_viagem
    GROUP BY
      1,
      2,
      3
  ),
  -- 9. Considera o validador com maior porcentagem de estado do equipamento "ABERTO" por viagem
  estado_equipamento_max_perc AS (
    SELECT
      data,
      id_viagem,
      MAX_BY(id_validador, percentual_estado_equipamento_aberto) AS id_validador,
      MAX(percentual_estado_equipamento_aberto) AS percentual_estado_equipamento_aberto
    FROM
      estado_equipamento_perc
    GROUP BY
      1,
      2
  ),
  -- 10. Verifica se a viagem possui estado do equipamento "ABERTO" em pelo menos 80% dos registros
  estado_equipamento_verificacao AS (
    SELECT
      data,
      id_viagem,
      id_validador,
      percentual_estado_equipamento_aberto,
      IF(percentual_estado_equipamento_aberto >= 0.8 OR percentual_estado_equipamento_aberto IS NULL, TRUE, FALSE) AS indicador_estado_equipamento_aberto
    FROM
      viagem
    LEFT JOIN
      estado_equipamento_max_perc
    USING
      (data, id_viagem)
  )
SELECT
  v.data,
  v.id_viagem,
  v.id_veiculo,
  v.servico,
  eev.id_validador,
  CASE
    WHEN v.data >= DATE("2024-10-01")
      AND (COALESCE(tr.quantidade_transacao_riocard, 0) = 0
        OR COALESCE(eev.indicador_estado_equipamento_aberto, FALSE) = FALSE)
      AND ve.status IN ("Licenciado com ar e não autuado", "Licenciado sem ar e não autuado")
      THEN "Sem transação"
    ELSE ve.status
  END AS tipo_viagem,
  v.sentido,
  v.distancia_planejada,
  COALESCE(t.quantidade_transacao, 0) AS quantidade_transacao,
  COALESCE(tr.quantidade_transacao_riocard, 0) AS quantidade_transacao_riocard,
  eev.percentual_estado_equipamento_aberto,
  eev.indicador_estado_equipamento_aberto,
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

