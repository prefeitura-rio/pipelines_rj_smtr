select
    parse_date("%d/%m/%Y", data) as data,
    trim(servico) as servico,
    safe_cast(incorporado_bigquery as bool) as incorporado_datalakehouse,
    * except (data, servico, incorporado_bigquery)
from
    {{
        source(
            "projeto_subsidio_sppo_encontro_contas_staging",
            "subtt_cmo_recurso_encontro_contas",
        )
    }}
