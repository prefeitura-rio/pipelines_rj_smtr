{# {{
    config(
        materialized="ephemeral",
    )
}} #}
-- Datas que serão desconsideradas no encontro de contas juntamente com o motivo
{% set datas_excecoes_dict = {
    "2024-10-06": "Eleições 2024",
} %}

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
    rdo_corrigido_agg as (
        select
            data,
            consorcio,
            servico,
            sum(receita_tarifaria_aferida) as receita_tarifaria_aferida
        from {{ ref("aux_rdo_servico_dia") }}
        group by all
    ),
    -- 3. Lista pares dia-serviço corrigidos do subsídio
    subsidio_dia_corrigido_agg as (
        select data, consorcio, servico, sum(km_apurada) as km_apurada
        from {{ ref("aux_subsidio_servico_dia") }}
        group by all
    ),
    -- 4. Associa pares dia-serviço subsidiados aos pares dia-serviço com receita
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
        where data not in ("{{ datas_excecoes_dict.keys() | join(", ") }}")  -- Remove datas de exceção que serão desconsideradas no encontro de contas
    )
-- 5. Inclui indicador de pares dia-serviço atípicos
select s.*, sda.data is not null as indicador_atipico
from subsidio_dia_rdo as s
left join servico_dia_atipico as sda using (data, servico)
