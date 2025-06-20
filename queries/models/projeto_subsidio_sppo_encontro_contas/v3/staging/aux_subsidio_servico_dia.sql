{# {{
    config(
        materialized="ephemeral",
    )
}} #}
with
    -- 3. Lista pares dia-serviço com correção do serviço (serviço corrigido é o
    -- serviço correto que deve ser utilizado no encontro de contas)
    correcao_servico_rdo as (select * from {{ ref("correcao_servico_rdo") }}),
    -- 6. Lista pares dia-serviço subsidiados
    subsidio_dia as (
        select data, consorcio, servico, km_apurada, perc_km_planejada
        from {{ ref("encontro_contas_subsidio_sumario_servico_dia_historico") }}
        where
            data >= "{{ var('DATA_SUBSIDIO_V9_INICIO') }}"  -- V2 válido apenas a partir da data de início da apuração por faixa horária
            and data between "{{ var('start_date') }}" and "{{ var('end_date') }}"
    ),
    -- 7. Filtra apenas pares dia-serviço com POD >= 80%
    subsidio_dia_filtrado as (select * from subsidio_dia where perc_km_planejada >= 80)
select
    s.* except (servico),
    servico as servico_original_subsidio,
    coalesce(servico_corrigido_subsidio, servico) as servico
from subsidio_dia_filtrado as s
left join
    (
        select
            * except (servico_corrigido, tipo),
            servico_corrigido as servico_corrigido_subsidio
        from correcao_servico_rdo
        where tipo = "Subsídio pago sem receita tarifária"
    ) using (data, servico)
