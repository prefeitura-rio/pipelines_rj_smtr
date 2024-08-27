{{
  config(
    materialized="incremental",
    partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    incremental_strategy="insert_overwrite",
  )
}}

WITH
  subsidio_faixa AS (
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
    pof
  FROM
    -- rj-smtr-dev.financeiro.subsidio_faixa_servico_dia
    {{ ref("subsidio_faixa_servico_dia") }}
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  ),
  subsidio_faixa_agg AS (
  SELECT
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    SUM(km_subsidiada_faixa) AS km_subsidiada_faixa,
    SUM(valor_apurado) AS valor_apurado,
    SUM(valor_acima_limite) AS valor_acima_limite,
    SUM(valor_total_sem_glosa) AS valor_total_sem_glosa
  FROM
    -- rj-smtr-dev.financeiro.subsidio_faixa_servico_dia_tipo_viagem
    {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  GROUP BY
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico
  ),
  pivot_data AS (
  SELECT
    *
  FROM (
    SELECT
      data,
      tipo_dia,
      faixa_horaria_inicio,
      faixa_horaria_fim,
      consorcio,
      servico,
      tipo_viagem,
      km_apurada_faixa
    FROM
      {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
      -- rj-smtr-dev.financeiro.subsidio_faixa_servico_dia_tipo_viagem
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
      "Sem transação" AS sem_transacao))
  )
SELECT
  s.data,
  s.tipo_dia,
  s.faixa_horaria_inicio,
  s.faixa_horaria_fim,
  s.consorcio,
  s.servico,
  s.viagens_faixa,
  s.km_apurada_faixa,
  agg.km_subsidiada_faixa,
  s.km_planejada_faixa,
  s.pof,
  pd.* EXCEPT(data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, servico, consorcio),
  agg.valor_apurado,
  agg.valor_acima_limite,
  agg.valor_total_sem_glosa
FROM
  subsidio_faixa AS s
LEFT JOIN
  subsidio_faixa_agg AS agg
USING(data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, consorcio, servico)
LEFT JOIN
  pivot_data AS pd
USING(data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, consorcio, servico)