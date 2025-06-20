with
    -- 1. Lista os pares dia-serviço subsidiados e com receita tarifária
    balanco_rdo_subsidio_servico_dia as (
        select
            data,
            consorcio,
            servico,
            receita_tarifaria_aferida,
            km_apurada,
            valor_valor_subsidio_pago
        from {{ ref("aux_balanco_rdo_subsidio_servico_dia") }}
        where indicador_atipico is false and receita_tarifaria_aferida is not null  -- Se não há receita tarifária, não entra para o Encontro de Contas
    ),
    -- 2. Lista os parâmetros de subsídio
    parametros as (
        select
            irk,
            irk_tarifa_publica,
            max(subsidio_km) as subsidio_km,
            min(data_inicio) as data_inicio,
            max(data_fim) as data_fim
        from {{ ref("valor_km_tipo_viagem") }}
        group by all
    )
select
    *,
    ifnull(receita_total_aferida, 0)
    - ifnull(receita_total_esperada - subsidio_glosado, 0) as saldo
from
    (
        select
            b.* except (valor_subsidio_pago),
            b.km_apurada * par.irk as receita_total_esperada,
            b.km_apurada * par.irk_tarifa_publica as receita_tarifaria_esperada,
            b.km_apurada * par.subsidio_km as subsidio_esperado,
            (b.km_apurada * par.subsidio_km - valor_subsidio_pago) as subsidio_glosado,
            ifnull(rdo.receita_tarifaria_aferida, 0)
            + ifnull(b.valor_subsidio_pago, 0) as receita_total_aferida,
            rdo.receita_tarifaria_aferida,
            b.valor_subsidio_pago
        from balanco_rdo_subsidio_servico_dia as b
        left join rdo using (data, servico)
        left join parametros as par on b.data between data_inicio and data_fim
    )
