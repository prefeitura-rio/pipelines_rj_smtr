{{
    config(
        materialized="table",
    )
}}

select *
from `rj-smtr-staging.br_rj_riodejaneiro_transporte_staging.estacoes_e_terminais_brt`
