with
    -- 1. Lista os pares dia-serviço subsidiados e com receita tarifária
    balanco_rdo_subsidio_servico_dia as (
        select
            data,
            consorcio,
            servico,
            receita_tarifaria_aferida,
            km_apurada,
            valor_subsidio_pago
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
    if(
        data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
        (
            ifnull(receita_total_aferida, 0)
            - ifnull(receita_total_esperada - subsidio_glosado, 0)
        ),
        (ifnull(receita_tarifaria_aferida, 0) - ifnull(receita_tarifaria_esperada, 0))
    ) as saldo
from
    (
        select
            b.* except (valor_subsidio_pago, receita_tarifaria_aferida),
            if(
                b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
                b.km_apurada * par.irk,
                null
            ) as receita_total_esperada,
            b.km_apurada * par.irk_tarifa_publica as receita_tarifaria_esperada,
            if(
                b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
                b.km_apurada * par.subsidio_km,
                null
            ) as subsidio_esperado,
            if(
                b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
                (b.km_apurada * par.subsidio_km - valor_subsidio_pago),
                null
            ) as subsidio_glosado,
            if(
                b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
                ifnull(b.receita_tarifaria_aferida, 0)
                + ifnull(b.valor_subsidio_pago, 0),
                null
            ) as receita_total_aferida,
            b.receita_tarifaria_aferida,
            if(
                b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
                b.valor_subsidio_pago,
                null
            ) as valor_subsidio_pago
        from balanco_rdo_subsidio_servico_dia as b
        left join parametros as par on b.data between data_inicio and data_fim
    )
