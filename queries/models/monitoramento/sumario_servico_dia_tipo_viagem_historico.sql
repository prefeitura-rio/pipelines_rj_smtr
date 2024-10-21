WITH
  pre_faixa_horaria AS (
  SELECT
    data,
    tipo_dia,
    consorcio,
    servico,
    tipo_viagem,
    indicador_ar_condicionado,
    viagens,
    km_apurada
  FROM
    {{ ref('sumario_servico_tipo_viagem_dia')}}
    -- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_tipo_viagem_dia`
  WHERE
    data < DATE("{{ var("DATA_SUBSIDIO_V9_INICIO") }}")
    ),
  pos_faixa_horaria AS (
  SELECT
    data,
    tipo_dia,
    consorcio,
    servico,
    tipo_viagem,
    indicador_ar_condicionado,
    sum(viagens_faixa) AS viagens,
    sum(km_apurada_faixa) AS km_apurada
  FROM
    {{ ref('subsidio_faixa_servico_dia_tipo_viagem')}}
    -- `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem`
  WHERE
    data >= DATE("{{ var("DATA_SUBSIDIO_V9_INICIO") }}")
    and tipo_viagem != "Sem viagem apurada"
  GROUP by data,
    tipo_dia,
    consorcio,
    servico,
    tipo_viagem,
    indicador_ar_condicionado)
SELECT
  *
FROM
  pre_faixa_horaria
UNION ALL
SELECT
  *
FROM
  pos_faixa_horaria