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
    SUM(km_planejada_faixa) AS km_planejada_dia,
    MIN(pof) AS min_pof
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
  subsidio_faixa AS (
  SELECT
    *
  FROM
    {{ ref("subsidio_faixa_servico_dia") }}
    -- rj-smtr-dev.financeiro.subsidio_faixa_servico_dia
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
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
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    indicador_ar_condicionado,
    tipo_viagem,
    viagens_faixa,
    km_apurada_faixa,
    km_subsidiada_faixa,
    valor_apurado,
    valor_acima_limite,
    valor_total_sem_glosa,
    valor_judicial
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
    -- CASE
    --   WHEN sf.pof >= 80 THEN SUM(s.valor_apurado)
    --   ELSE pe.valor_penalidade
    -- END AS valor_total_com_glosa,
    SUM(s.valor_apurado) + pe.valor_penalidade AS valor_total_com_glosa,
    COALESCE(SUM(s.valor_judicial), 0) AS valor_judicial
  FROM
    subsidio_dia_tipo_viagem AS s
  -- LEFT JOIN
  --   subsidio_faixa AS sf
  -- USING(data, tipo_dia, consorcio, servico)
  LEFT JOIN
    penalidade AS pe
  USING(data, tipo_dia, consorcio, servico)
  GROUP BY
    s.data,
    s.tipo_dia,
    s.consorcio,
    s.servico,
    -- sf.pof,
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
  CASE
    WHEN sd.min_pof >= 80 THEN
      (vc.valor_total_com_glosa - vc.valor_total_sem_glosa)
    ELSE
      vc.valor_penalidade
  END AS valor_glosado,
  -- vc.valor_total_com_glosa - vc.valor_total_sem_glosa AS valor_glosado,
  -vc.valor_acima_limite AS valor_acima_limite,
  vc.valor_total_sem_glosa,
  vc.valor_total_com_glosa + vc.valor_acima_limite -
  CASE
    WHEN sd.min_pof >= 80 THEN
      (vc.valor_total_com_glosa - vc.valor_penalidade - vc.valor_total_sem_glosa)
    ELSE
      vc.valor_penalidade
  END AS valor_total_apurado,
  -- vc.valor_total_com_glosa + vc.valor_acima_limite - (vc.valor_total_com_glosa - vc.valor_penalidade - vc.valor_total_sem_glosa) AS valor_total_apurado,
  vc.valor_judicial,
  vc.valor_penalidade
FROM
  subsidio_dia AS sd
LEFT JOIN
  valores_calculados AS vc
USING(data, tipo_dia, consorcio, servico)
-- LEFT JOIN
--   subsidio_faixa AS sf
-- USING(data, tipo_dia, consorcio, servico)