{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

WITH
-- 1. Viagens planejadas
  planejado AS (
  SELECT
    DISTINCT data,
    tipo_dia,
    consorcio,
    servico,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    TRUNC(distancia_total_planejada, 3) AS km_planejada
  FROM
    {{ ref("viagem_planejada") }}
    -- rj-smtr.projeto_subsidio_sppo.viagem_planejada
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
    AND distancia_total_planejada > 0
  ),
-- 2. Viagens realizadas
  viagem AS (
  SELECT
    data,
    servico_realizado AS servico,
    id_viagem,
    datetime_partida,
    distancia_planejada
 FROM
    {{ ref("viagem_completa") }}
    -- rj-smtr.projeto_subsidio_sppo.viagem_completa
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  ),
-- 3. Apuração de km realizado e Percentual de Operação por faixa
  servico_km_apuracao AS (
  SELECT
    p.data,
    p.tipo_dia,
    p.faixa_horaria_inicio,
    p.faixa_horaria_fim,
    p.consorcio,
    p.servico,
    SAFE_CAST(p.km_planejada AS NUMERIC) AS km_planejada_faixa,
    SAFE_CAST(COALESCE(COUNT(v.id_viagem), 0) AS INT64) AS viagens_faixa,
    SAFE_CAST(COALESCE(SUM(v.distancia_planejada), 0) AS NUMERIC) AS km_apurada_faixa,
    SAFE_CAST(COALESCE(ROUND(100 * SUM(v.distancia_planejada) / p.km_planejada, 2), 0) AS NUMERIC) AS pof
  FROM
    planejado AS p
  LEFT JOIN
    viagem AS v
  ON
    p.data = v.data
    AND p.servico = v.servico
    AND v.datetime_partida BETWEEN p.faixa_horaria_inicio
    AND p.faixa_horaria_fim
  GROUP BY
    p.data, p.tipo_dia, p.faixa_horaria_inicio, p.faixa_horaria_fim, p.consorcio, p.servico, p.km_planejada
)
SELECT
  data,
  tipo_dia,
  faixa_horaria_inicio,
  faixa_horaria_fim,
  consorcio,
  servico,
  viagens_faixa,
  km_apurada_faixa,
  km_planejada_faixa,
  pof,
  '{{ var("version") }}' as versao,
  CURRENT_DATETIME("America/Sao_Paulo") as datetime_ultima_atualizacao
FROM
  servico_km_apuracao