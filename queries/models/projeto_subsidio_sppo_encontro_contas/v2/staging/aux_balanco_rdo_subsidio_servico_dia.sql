{# {{
    config(
        materialized="ephemeral",
    )
}} #}
with
    -- 1. Lista pares dia-serviço atípicos (recurso pago/em avaliação e ainda não
    -- incorporados
    -- ao data lakehouse)
    servico_dia_atipico as (
        select distinct data, servico
        from {{ ref("servico_dia_atipico") }}
        where incorporado_datalakehouse is not true
    ),
    -- 2. Lista pares dia-serviço corrigidos do RDO
    rdo_corrigido_agg as (
        select
            data,
            consorcio,
            servico,
            sum(receita_tarifaria_aferida) as receita_tarifaria_aferida
        from {{ ref("aux_rdo_servico_dia") }}
        group by all
    ),
    -- 3. Filtra apenas pares dia-serviço com POD >= 80%
    subsidio_dia_filtrado as (
        select *
        from {{ ref("aux_subsidio_servico_dia") }}
        where perc_km_planejada >= 80
    ),
    -- 4. Lista pares dia-serviço corrigidos do subsídio
    subsidio_dia_corrigido_agg as (
        select
            data,
            consorcio,
            servico,
            sum(km_apurada) as km_apurada,
            sum(valor_subsidio_pago) as valor_subsidio_pago
        from subsidio_dia_filtrado
        group by all
    ),
    -- 5. Associa pares dia-serviço subsidiados aos pares dia-serviço com receita
    -- tarifária aferida
    subsidio_dia_rdo as (
        select
            data,
            coalesce(sd.consorcio, rdo.consorcio) as consorcio,
            servico,
            sd.* except (data, consorcio, servico),
            rdo.* except (data, consorcio, servico)
        from subsidio_dia_corrigido_agg as sd
        left join rdo_corrigido_agg as rdo using (data, servico)
        where
            data
            not in ({{ var("encontro_contas_datas_excecoes").keys() | join(", ") }})  -- Remove datas de exceção que serão desconsideradas no encontro de contas
    )
-- 6. Inclui indicador de pares dia-serviço atípicos
select s.*, sda.data is not null as indicador_atipico
from subsidio_dia_rdo as s
left join servico_dia_atipico as sda using (data, servico)
