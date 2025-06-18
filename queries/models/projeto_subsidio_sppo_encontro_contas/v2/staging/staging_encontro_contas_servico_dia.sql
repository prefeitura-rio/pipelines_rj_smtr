/*
- km_apurada_pod: soma das kms por tipo e por faixa horária que serão consideradas para o cálculo do POD (Percentual de Operação Diária) conforme rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem
    - Além das kms apuradas normalmente, considera também as kms não vistoriadas (2024-08-16 a 2024-09-30)
- km_apurada: soma das kms por faixa horária que foram apuradas e utilizadas para o cálculo do subsídio conforme tabelas rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia e rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento
- km_planejada: soma das kms por faixa horária que foram planejadas e utilizadas para o cálculo do subsídio conforme tabelas rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia e rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento
- perc_km_planejada: percentual de km_apurada_pod em relação a km_planejada, ou seja, o POD (Percentual de Operação Diária) do serviço
*/
{# {% set subsidio_faixa_servico_dia_tipo_viagem = ref("subsidio_faixa_servico_dia_tipo_viagem") %} #}
{# {% set sumario_faixa_servico_dia = ref("sumario_faixa_servico_dia") %} #}
{# {% set sumario_faixa_servico_dia_pagamento = ref("sumario_faixa_servico_dia_pagamento") %} #}
{% set subsidio_faixa_servico_dia_tipo_viagem = (
    "rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem"
) %}
{% set sumario_faixa_servico_dia = (
    "rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia"
) %}
{% set sumario_faixa_servico_dia_pagamento = (
    "rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento"
) %}

{{ config(materialized="ephemeral") }}

with
    sumario_servico_dia_pod as (
        select
            data,
            servico,
            sum(
                case
                    when
                        data >= date("{{ var('DATA_SUBSIDIO_V9A_INICIO') }}")
                        and tipo_viagem not in ("Não licenciado", "Não vistoriado")
                    then km_apurada_faixa
                    when
                        data < date("{{ var('DATA_SUBSIDIO_V9A_INICIO') }}")
                        and tipo_viagem not in ("Não licenciado")
                    then km_apurada_faixa
                    else 0
                end
            ) as km_apurada_pod,
        from {{ subsidio_faixa_servico_dia_tipo_viagem }}
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
        group by all
    ),
    sumario_faixa_servico as (
        select data, consorcio, servico, km_apurada_faixa, km_planejada_faixa,
        from {{ sumario_faixa_servico_dia }}
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
        union all
        select data, consorcio, servico, km_apurada_faixa, km_planejada_faixa,
        from {{ sumario_faixa_servico_dia_pagamento }} as sdp
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data >= date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
    ),
    sumario_servico_dia as (
        select
            data,
            consorcio,
            servico,
            sum(km_apurada_faixa) as km_apurada,
            sum(km_planejada_faixa) as km_planejada,
        from sumario_faixa_servico
        group by all
    )
select
    data,
    servico,
    consorcio,
    km_apurada_pod,
    km_apurada,
    km_planejada,
    round(100 * km_apurada_pod / km_planejada, 2) as perc_km_planejada,
from sumario_servico_dia as ssd
left join sumario_servico_dia_pod as ssdp using (data, servico)
