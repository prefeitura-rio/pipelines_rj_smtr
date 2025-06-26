{{
    config(
        materialized="ephemeral",
    )
}}

select data, id_veiculo, placa, tecnologia, status, indicadores
from {{ ref("sppo_veiculo_dia") }}

union all

select data, id_veiculo, placa, tecnologia, status, indicadores
from {{ ref("veiculo_dia") }}
where modo is null or (modo = 'ONIBUS' and tipo_veiculo not like '%ROD%')
