{{
    config(
        materialized="table",
    )
}}

select *
from
    {{
        source(
            "br_rj_riodejaneiro_transporte_staging", "terminais_onibus_coordenadas"
        )
    }}
