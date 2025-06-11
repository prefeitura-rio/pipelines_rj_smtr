{{ config(materialized="ephemeral") }}
select
    * except (data, receita_tarifaria, tipo),
    parse_date("%d/%m/%Y", data) as data,
    safe_cast(
        replace(regexp_replace(receita_tarifaria, r"[^\d,]", ""), ",", ".") as numeric
    ) as receita_tarifaria_aferida_rdo,
    trim(tipo) as tipo
from
    {{
        source(
            "projeto_subsidio_sppo_encontro_contas_staging",
            "rdo_rioonibus_correcao_servico_2024_2025",
        )
    }}
