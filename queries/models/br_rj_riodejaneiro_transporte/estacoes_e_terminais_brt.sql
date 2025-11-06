{{
    config(
        materialized="table",
    )
}}

select *
from `rj-smtr.br_rj_riodejaneiro_transporte.terminais_onibus_coordenadas`
