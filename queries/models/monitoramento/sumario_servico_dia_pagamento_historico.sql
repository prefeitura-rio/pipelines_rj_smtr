WITH
  pre_faixa_horaria AS (
  SELECT
    data,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens,
    km_apurada,
    km_planejada,
    perc_km_planejada,
    valor_subsidio_pago,
    valor_penalidade
  FROM
    {{ ref('sumario_servico_dia_historico')}}
    -- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico`
  WHERE
    data < DATE("{{ var("DATA_SUBSIDIO_V9_INICIO") }}") ),
  planejada AS (
  SELECT
    DISTINCT data,
    consorcio,
    servico,
    vista
  FROM
    {{ ref('viagem_planejada')}}
    -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
  WHERE
    data >= DATE("{{ var("DATA_SUBSIDIO_V9_INICIO") }}")
    AND (id_tipo_trajeto = 0
      OR id_tipo_trajeto IS NULL)
    AND FORMAT_TIME("%T", TIME(faixa_horaria_inicio)) != "00:00:00" ),
  pagamento AS (
  SELECT
    data,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens_dia AS viagens,
    CASE
      WHEN data >= DATE("{{ var("DATA_SUBSIDIO_V10_INICIO") }}") THEN COALESCE(km_apurada_registrado_com_ar_inoperante,0) + COALESCE(km_apurada_autuado_ar_inoperante,0) + COALESCE(km_apurada_autuado_seguranca,0) + COALESCE(km_apurada_autuado_limpezaequipamento,0) + COALESCE(km_apurada_licenciado_sem_ar_n_autuado,0) + COALESCE(km_apurada_licenciado_com_ar_n_autuado,0) + COALESCE(km_apurada_sem_transacao, 0)
      ELSE COALESCE(km_apurada_registrado_com_ar_inoperante,0) + COALESCE(km_apurada_autuado_ar_inoperante,0) + COALESCE(km_apurada_autuado_seguranca,0) + COALESCE(km_apurada_autuado_limpezaequipamento,0) + COALESCE(km_apurada_licenciado_sem_ar_n_autuado,0) + COALESCE(km_apurada_licenciado_com_ar_n_autuado,0) + COALESCE(km_apurada_sem_transacao, 0) + COALESCE(km_apurada_n_vistoriado, 0) + COALESCE(km_apurada_n_licenciado, 0)
  END
    AS km_apurada,
    km_planejada_dia AS km_planejada,
    valor_a_pagar AS valor_subsidio_pago,
    valor_penalidade
  FROM
    {{ ref('sumario_servico_dia_pagamento')}}
    -- `rj-smtr.dashboard_subsidio_sppo_v2.sumario_servico_dia_pagamento`
  LEFT JOIN
    planejada
  USING
    (data,
      servico,
      consorcio)
  WHERE
    data >= "2024-08-16" ),
  pos_faixa_horaria AS (
  SELECT
    data,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens,
    km_apurada,
    km_planejada,
    100*km_apurada/km_planejada AS perc_km_planejada,
    valor_subsidio_pago,
    valor_penalidade
  FROM
    pagamento )
SELECT
  *
FROM
  pre_faixa_horaria
UNION ALL
SELECT
  *
FROM
  pos_faixa_horaria