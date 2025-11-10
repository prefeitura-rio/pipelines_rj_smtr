{{
    config(
        materialized="table",
    )
}}

select *
from `rj-smtr-staging.br_rj_riodejaneiro_transporte_staging.terminais_onibus_coordenadas`
