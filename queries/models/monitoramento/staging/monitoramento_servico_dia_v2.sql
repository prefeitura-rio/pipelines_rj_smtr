{{ config(materialized="ephemeral") }}

with
  valores_subsidio AS (
SELECT
  data,
  sdp.tipo_dia,
  sdp.consorcio,
  servico,
  sdp.viagens_dia,
  SUM(
    CASE
      WHEN data >= DATE("2024-09-01") 
           AND tipo_viagem NOT IN ("Não licenciado", "Não vistoriado") 
      THEN km_apurada_faixa
      WHEN data < DATE("2024-09-01")
      THEN km_apurada_faixa
      ELSE 0
    END
  ) AS km_apurada,
  km_planejada_dia,
  valor_a_pagar,
  valor_penalidade
FROM
  {{ ref("subsidio_sumario_servico_dia_pagamento") }} as sdp
    -- rj-smtr.financeiro.subsidio_sumario_servico_dia_pagamento
left join {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }} as sdtv
  using (data, servico)
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  group by data,
  tipo_dia,
  consorcio,
  servico,
  viagens_dia,
  km_planejada_dia,
  valor_a_pagar,
  valor_penalidade
  ),
    planejada as (
        select distinct data, consorcio, servico, vista
        from {{ ref("viagem_planejada") }}
        -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
        where
            data >= date("{{ var("DATA_SUBSIDIO_V9_INICIO") }}")
            and (id_tipo_trajeto = 0 or id_tipo_trajeto is null)
            and format_time("%T", time(faixa_horaria_inicio)) != "00:00:00"
    ),
    pagamento as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            vista,
            viagens_dia as viagens,
            km_apurada,
            km_planejada_dia as km_planejada,
            valor_a_pagar as valor_subsidio_pago,
            valor_penalidade
        from valores_subsidio as sdp
        left join planejada as p using (data, servico, consorcio)
        where
            data >= date("{{ var("DATA_SUBSIDIO_V9_INICIO") }}")
            {% if is_incremental() %}
                and data between date("{{ var("start_date") }}") and date_add(
                    date("{{ var("end_date") }}"), interval 1 day
                )
            {% endif %}
    )
select
    data,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens,
    km_apurada,
    km_planejada,
    ROUND(100 * km_apurada / km_planejada, 2) as perc_km_planejada,
    valor_subsidio_pago,
    valor_penalidade
from pagamento
