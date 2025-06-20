with
    -- 1. Lista os pares dia-serviço subsidiados e com receita tarifária
    balanco_rdo_servico_dia as (select * from {{ ref("aux_balanco_rdo_servico_dia") }})
    -- 2. Lista os parâmetros de subsídio
    parametros as (
        select
            irk_tarifa_publica,
            min(data_inicio) as data_inicio,
            max(data_fim) as data_fim
        from {{ ref("valor_km_tipo_viagem") }}
        group by all
    )
select
    *,
    ifnull(receita_tarifaria_aferida, 0)
    - ifnull(receita_tarifaria_esperada, 0) as saldo
from
    (
        select
            * except (subsidio_pago),
            km_apurada * par.irk_tarifa_publica as receita_tarifaria_esperada,
        from balanco_rdo_servico_dia
        left join parametros par on ks.data between data_inicio and data_fim
    )
