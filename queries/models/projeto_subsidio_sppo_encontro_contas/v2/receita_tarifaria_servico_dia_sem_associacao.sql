with
    -- 1. Lista pares dia-serviço atípicos (recurso pago/em avaliação e ainda não
    -- incorporados
    -- ao data lakehouse)
    servico_dia_atipico as (
        select distinct data, servico
        from {{ ref("recurso_encontro_contas") }}
        where incorporado_datalakehouse is not true
    ),
    -- 2. Lista pares dia-serviço corrigidos do RDO
    rdo_corrigido as (select * from {{ ref("aux_rdo_servico_dia") }}),
    -- 3. Lista pares dia-serviço corrigidos do subsídio
    subsidio_dia_corrigido as (select * from {{ ref("aux_subsidio_servico_dia") }})
-- 4. Lista os pares dia-serviço que estão sem planejamento porém com receita
-- tarifária ou tem subsídio pago sem receita tarifária aferida
select
    case
        when rdo.servico is not null and sd.servico is null
        then "Sem planejamento porém com receita tarifária"
        when rdo.servico is null and sd.servico is not null
        then "Subsídio pago sem receita tarifária"
        else null
    end as tipo,
    data,
    coalesce(sd.consorcio, rdo.consorcio) as consorcio,
    servico,
    linha,
    servico_original_rdo,
    servico_original_subsidio,
    tipo_servico,
    ordem_servico,
    receita_tarifaria_aferida,
    null as justificativa,
    null as servico_correto
from subsidio_dia_corrigido as sd
full join rdo_corrigido as rdo using (data, servico)
where
    (
        (perc_km_planejada >= 80 and receita_tarifaria_aferida is null)
        or (receita_tarifaria_aferida is not null and perc_km_planejada is null)
    )
    and data not in ({{ var("encontro_contas_datas_excecoes").keys() | join(", ") }})  -- Remove datas de exceção que serão desconsideradas no encontro de contas
