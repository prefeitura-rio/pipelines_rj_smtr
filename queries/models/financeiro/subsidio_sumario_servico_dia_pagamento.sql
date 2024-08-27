{{
  config(
    materialized="incremental",
    partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    incremental_strategy="insert_overwrite",
  )
}}

WITH
  subsidio_dia AS (
  SELECT
    data,
    tipo_dia,
    consorcio,
    servico,
    SUM(viagens_faixa) AS viagens_dia,
    SUM(km_apurada_faixa) AS km_apurada_dia,
    SUM(km_planejada_faixa) AS km_planejada_dia
  FROM
    {{ ref("subsidio_faixa_servico_dia") }}
    -- rj-smtr-dev.financeiro.subsidio_faixa_servico_dia
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  GROUP BY
    data,
    tipo_dia,
    consorcio,
    servico
  ),
  -- subsidio_faixa AS (
  -- SELECT
  --   *
  -- FROM
  --   {{ ref("subsidio_faixa_servico_dia") }}
  --   -- rj-smtr-dev.financeiro.subsidio_faixa_servico_dia
  -- WHERE
  --   data BETWEEN DATE("{{ var("start_date") }}")
  --   AND DATE("{{ var("end_date") }}")
  -- ),
  subsidio_parametros AS (
  SELECT
    DISTINCT data_inicio,
    data_fim,
    status,
    subsidio_km,
    MAX(subsidio_km) OVER (PARTITION BY data_inicio, data_fim) AS subsidio_km_teto
  FROM
    {{ ref("subsidio_valor_km_tipo_viagem") }}
    -- rj-smtr-staging.dashboard_subsidio_sppo_staging.subsidio_valor_km_tipo_viagem
),
  penalidade AS (
  SELECT
    data,
    tipo_dia,
    consorcio,
    servico,
    valor_penalidade
  FROM
    {{ ref("subsidio_penalidade_servico_dia") }}
  ),
  subsidio_dia_tipo_viagem AS (
  SELECT
    *
  FROM
    -- rj-smtr-dev.financeiro.subsidio_faixa_servico_dia_tipo_viagem
    {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  ),
  valores_calculados AS (
  SELECT
    s.data,
    s.tipo_dia,
    s.consorcio,
    s.servico,
    pe.valor_penalidade,
    SUM(s.km_subsidiada_faixa) AS km_subsidiada_dia,
    COALESCE(SUM(s.valor_acima_limite), 0) AS valor_acima_limite,
    COALESCE(SUM(s.valor_total_sem_glosa), 0) AS valor_total_sem_glosa,
    SUM(s.valor_apurado) + pe.valor_penalidade AS valor_total_com_glosa,
    CASE
      WHEN pe.valor_penalidade != 0 THEN -pe.valor_penalidade
      ELSE SAFE_CAST(TRUNC((SUM(IF(indicador_viagem_dentro_limite = TRUE AND indicador_penalidade_judicial = TRUE, km_apurada_faixa*subsidio_km_teto, 0))
           - SUM(IF(indicador_viagem_dentro_limite = TRUE AND indicador_penalidade_judicial = TRUE, km_apurada_faixa*subsidio_km, 0))), 2) AS NUMERIC)
    END AS valor_judicial,
  FROM
    subsidio_dia_tipo_viagem AS s
  LEFT JOIN
    penalidade AS pe
  USING(data, tipo_dia, consorcio, servico)
  LEFT JOIN
    subsidio_parametros AS sp
  ON
    s.data BETWEEN sp.data_inicio
    AND sp.data_fim
    AND s.tipo_viagem = sp.status
  GROUP BY
    s.data,
    s.tipo_dia,
    s.consorcio,
    s.servico,
    pe.valor_penalidade
  )
SELECT
  sd.data,
  sd.tipo_dia,
  sd.consorcio,
  sd.servico,
  sd.viagens_dia,
  sd.km_apurada_dia,
  vc.km_subsidiada_dia,
  sd.km_planejada_dia,
  vc.valor_total_com_glosa AS valor_a_pagar,
  vc.valor_total_com_glosa - vc.valor_total_sem_glosa AS valor_glosado,
  vc.valor_acima_limite,
  vc.valor_total_sem_glosa,
  vc.valor_acima_limite + vc.valor_penalidade + vc.valor_total_sem_glosa AS valor_total_apurado,
  vc.valor_judicial,
  vc.valor_penalidade
FROM
  subsidio_dia AS sd
LEFT JOIN
  valores_calculados AS vc
USING(data, tipo_dia, consorcio, servico)