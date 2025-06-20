{# {{
    config(
        materialized="ephemeral",
    )
}} #}
with
    -- 1. Lista pares dia-serviço com correção do serviço (serviço corrigido é o
    -- serviço correto que deve ser utilizado no encontro de contas)
    correcao_servico_rdo as (
        select *
        from {{ ref("correcao_servico_rdo") }}
        where tipo = "Subsídio pago sem receita tarifária"
    ),
    -- 2. Lista pares dia-serviço subsidiados
    subsidio_dia as (
        select
            data, consorcio, servico, km_apurada, perc_km_planejada, valor_subsidio_pago
        from {{ ref("encontro_contas_subsidio_sumario_servico_dia_historico") }}
        where data between "{{ var('start_date') }}" and "{{ var('end_date') }}"
    )
-- 3. Associa serviço corrigido aos pares dia-serviço do subsídio
select
    s.* except (servico),
    servico as servico_original_subsidio,
    coalesce(servico_corrigido, servico) as servico
from subsidio_dia as s
left join correcao_servico_rdo using (data, servico)
