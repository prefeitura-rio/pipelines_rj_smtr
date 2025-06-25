with
    -- 1. Lista os pares dia-serviço subsidiados e com receita tarifária
    balanco_rdo_servico_dia as (
        select data, consorcio, servico, receita_tarifaria_aferida, km_apurada,
        from {{ ref("aux_balanco_rdo_subsidio_servico_dia") }}
        where indicador_atipico is false and receita_tarifaria_aferida is not null  -- Se não há receita tarifária, não entra para o Encontro de Contas
    ),
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
            * except (data_inicio, data_fim),
            km_apurada * par.irk_tarifa_publica as receita_tarifaria_esperada,
        from balanco_rdo_servico_dia as b
        left join parametros as par on data between data_inicio and data_fim
    )
