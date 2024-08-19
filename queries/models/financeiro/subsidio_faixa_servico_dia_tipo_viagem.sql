{{
  config(
    materialized="incremental",
    partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    incremental_strategy="insert_overwrite",
  )
}}

WITH
  subsidio_faixa_dia AS (
  SELECT
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    pof
  FROM
    rj-smtr-dev.financeiro.subsidio_faixa_servico_dia
    --{{ ref("subsidio_faixa_servico_dia") }}
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  ),
  servico_km_apuracao AS (
  SELECT
    data,
    servico,
    CASE
      WHEN tipo_viagem = "Nao licenciado" THEN "Não licenciado"
      WHEN tipo_viagem = "Licenciado com ar e autuado (023.II)" THEN "Autuado por ar inoperante"
      WHEN tipo_viagem = "Licenciado sem ar" THEN "Licenciado sem ar e não autuado"
      WHEN tipo_viagem = "Licenciado com ar e não autuado (023.II)" THEN "Licenciado com ar e não autuado"
      ELSE tipo_viagem
    END AS tipo_viagem,
    id_viagem,
    distancia_planejada,
    subsidio_km,
    -- subsidio_km_teto,
    4.04 AS subsidio_km_teto,
    indicador_viagem_remunerada
  FROM
    {{ ref("viagens_remuneradas") }}
    -- rj-smtr.dashboard_subsidio_sppo.viagens_remuneradas
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  ),
  indicador_ar AS (
  SELECT
    data,
    id_veiculo,
    status,
    SAFE_CAST(JSON_VALUE(indicadores,"$.indicador_ar_condicionado") AS BOOL) AS indicador_ar_condicionado
  FROM
    -- {{ ref("sppo_veiculo_dia") }}
    rj-smtr.veiculo.sppo_veiculo_dia
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  ),
  viagem AS (
  SELECT
    data,
    servico_realizado AS servico,
    id_veiculo,
    id_viagem
  FROM
    {{ ref("viagem_completa") }}
    -- rj-smtr.projeto_subsidio_sppo.viagem_completa
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  ),
  ar_viagem AS (
  SELECT
    v.data,
    v.servico,
    v.id_viagem,
    COALESCE(ia.indicador_ar_condicionado, FALSE) AS indicador_ar_condicionado
  FROM
    viagem v
  LEFT JOIN
    indicador_ar ia
  ON
    ia.data = v.data
    AND ia.id_veiculo = v.id_veiculo
  ),
  subsidio_servico_ar AS (
    SELECT
      sfd.data,
      sfd.tipo_dia,
      sfd.faixa_horaria_inicio,
      sfd.faixa_horaria_fim,
      sfd.consorcio,
      sfd.servico,
      sfd.pof,
      COALESCE(s.tipo_viagem, "Sem viagem apurada") AS tipo_viagem,
      s.id_viagem,
      s.distancia_planejada,
      s.subsidio_km,
      s.subsidio_km_teto,
      s.indicador_viagem_remunerada,
      CASE
        WHEN s.tipo_viagem = "Autuado por ar inoperante" THEN TRUE
        WHEN s.tipo_viagem = "Licenciado sem ar e não autuado" THEN TRUE
        WHEN sfd.pof < 60 THEN TRUE
        WHEN s.tipo_viagem = "Registrado com ar inoperante" THEN TRUE
        ELSE FALSE
      END AS indicador_penalidade_judicial,
      COALESCE(av.indicador_ar_condicionado, FALSE) AS indicador_ar_condicionado
    FROM
      subsidio_faixa_dia AS sfd
    LEFT JOIN
      servico_km_apuracao AS s
    ON
      sfd.data = s.data
      AND sfd.servico = s.servico
    LEFT JOIN
      ar_viagem AS av
    ON
      s.data = av.data
      AND s.servico = av.servico
      AND s.id_viagem = av.id_viagem
  )
SELECT
  data,
  tipo_dia,
  faixa_horaria_inicio,
  faixa_horaria_fim,
  consorcio,
  servico,
  indicador_ar_condicionado,
  tipo_viagem,
  SAFE_CAST(COALESCE(COUNT(id_viagem), 0) AS INT64) AS viagens_faixa,
  SAFE_CAST(COALESCE(SUM(distancia_planejada), 0) AS NUMERIC) AS km_apurada_faixa,
  SAFE_CAST(COALESCE(SUM(IF(tipo_viagem != "Não licenciado", distancia_planejada, 0)), 0) AS NUMERIC) AS km_subsidiada_faixa,
  SAFE_CAST(SUM(IF(indicador_viagem_remunerada = TRUE AND pof >= 80, distancia_planejada*subsidio_km, 0)) AS NUMERIC) AS valor_apurado,
  SAFE_CAST(COALESCE(SUM(IF(indicador_viagem_remunerada = TRUE, 0, distancia_planejada*subsidio_km)), 0) AS NUMERIC) AS valor_acima_limite,
  SAFE_CAST(SUM(IF(pof >= 80 AND tipo_viagem != "Não licenciado", distancia_planejada*subsidio_km_teto, 0)) - COALESCE(SUM(IF(indicador_viagem_remunerada = TRUE, 0, distancia_planejada*subsidio_km)), 0) AS NUMERIC) AS valor_total_sem_glosa,
  SAFE_CAST((SUM(IF(indicador_viagem_remunerada = TRUE AND indicador_penalidade_judicial = TRUE, distancia_planejada*subsidio_km_teto, 0))
  - SUM(IF(indicador_viagem_remunerada = TRUE AND indicador_penalidade_judicial = TRUE, distancia_planejada*subsidio_km, 0))) AS NUMERIC) AS valor_judicial
FROM
  subsidio_servico_ar
GROUP BY
  data,
  tipo_dia,
  faixa_horaria_inicio,
  faixa_horaria_fim,
  consorcio,
  servico,
  pof,
  indicador_ar_condicionado,
  tipo_viagem
