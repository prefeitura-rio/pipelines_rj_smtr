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
    SAFE_CAST(AVG(pof) AS NUMERIC) AS media_pof,
    SAFE_CAST(STDDEV(pof) AS NUMERIC) AS desvp_pof
  FROM
    -- {{ ref("subsidio_faixa_servico_dia") }}
    rj-smtr-dev.financeiro.subsidio_faixa_servico_dia
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  GROUP BY
    data,
    tipo_dia,
    consorcio,
    servico
  ),
  valores_subsidio AS (
  SELECT
    *
  FROM
    rj-smtr-dev.financeiro.subsidio_sumario_servico_dia_pagamento
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  ),
  pivot_data AS (
  SELECT
    *
  FROM (
    SELECT
      data,
      tipo_dia,
      consorcio,
      servico,
      tipo_viagem,
      km_apurada_faixa
    FROM
      rj-smtr-dev.financeiro.subsidio_faixa_servico_dia_tipo_viagem
    WHERE
      data BETWEEN DATE("{{ var("start_date") }}")
      AND DATE("{{ var("end_date") }}")
    )
    PIVOT(SUM(km_apurada_faixa) AS km_apurada FOR tipo_viagem IN (
      "Registrado com ar inoperante" AS registrado_com_ar_inoperante,
      "Não licenciado" AS n_licenciado,
      "Autuado por ar inoperante" AS autuado_ar_inoperante,
      "Autuado por segurança" AS autuado_seguranca,
      "Autuado por limpeza/equipamento" AS autuado_limpezaequipamento,
      "Licenciado sem ar e não autuado" AS licenciado_sem_ar_n_autuado,
      "Licenciado com ar e não autuado" AS licenciado_com_ar_n_autuado,
      "Não vistoriado" AS n_vistoriado,
      "Sem passageiro" AS sem_passageiro))
  )
SELECT
  vs.data,
  vs.tipo_dia,
  vs.consorcio,
  vs.servico,
  vs.viagens_dia,
  vs.km_apurada_dia,
  vs.km_subsidiada_dia,
  vs.km_planejada_dia,
  sd.media_pof,
  sd.desvp_pof,
  pd.* EXCEPT(data, tipo_dia, servico, consorcio),
  vs.valor_a_pagar,
  vs.valor_glosado,
  vs.valor_acima_limite,
  vs.valor_total_sem_glosa,
  vs.valor_total_apurado,
  vs.valor_judicial,
  vs.valor_penalidade
FROM
  valores_subsidio AS vs
LEFT JOIN
  subsidio_dia AS sd
USING(data, tipo_dia, consorcio, servico)
LEFT JOIN
  pivot_data AS pd
USING(data, tipo_dia, consorcio, servico)