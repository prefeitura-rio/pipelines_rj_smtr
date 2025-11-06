{{
    config(
        materialized="table",
    )
}}

select *
from `rj-smtr.br_rj_riodejaneiro_transporte.estacoes_e_terminais_brt`
