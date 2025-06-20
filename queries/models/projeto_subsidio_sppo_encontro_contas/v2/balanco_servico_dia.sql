with
    -- 1. Lista os pares dia-serviço subsidiados e com receita tarifária
    balanco_rdo_servico_dia as (
        select
            data,
            consorcio,
            servico_sumario,
            servico_rdo,
            servico_corrigido_sumario,
            servico_corrigido_rdo,
            receita_tarifaria_aferida,
            km_apurada,
            case
                when servico_corrigido_sumario is not null
                then servico_corrigido_sumario
                when servico_corrigido_rdo is not null
                then servico_corrigido_rdo
                else servico_sumario
            end as servico
        from {{ ref("aux_balanco_rdo_servico_dia") }}
        where indicador_atipico is false
    ),
    /*
    -- servico_corrigido_sumario: servico_corrigido > tipo = "Subsídio pago sem receita tarifária"
-- servico_corrigido_rdo: servico_corrigido > tipo = "Sem planejamento porém com receita tarifária"
-- servico_sumario: serviço original do subsídio
-- servico_rdo: serviço original do RDO


    Quando eu tenho servico_corrigido_sumario, pego a km do servico_sumario e jogo para o servico_corrigido_sumario
    Quando eu tenho servico_corrigido_rdo, pego a receita do servico_rdo e jogo para o servico_corrigido_rdo

    Expectativa: servico_corrigido obrigatoriamente precisa estar subsidiado, se não tiver, a receita tarifária entra para o consórcio
*/
    balanco_rdo_servico_dia_agg as (
        select
            data,
            consorcio,
            servico,
            sum(km_apurada) as km_apurada,
            sum(receita_tarifaria_aferida) as receita_tarifaria_aferida,
        from balanco_rdo_servico_dia
        group by all
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
        select *, km_apurada * par.irk_tarifa_publica as receita_tarifaria_esperada,
        from balanco_rdo_servico_dia_agg
        left join parametros as par on data between data_inicio and data_fim
    )
