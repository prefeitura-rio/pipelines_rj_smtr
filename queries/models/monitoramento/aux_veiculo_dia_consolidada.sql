{{
    config(
        materialized="ephemeral",
    )
}}

select data, id_veiculo, placa, null as ano_fabricacao, tecnologia, status, indicadores
from {{ ref("sppo_veiculo_dia") }}
{# from rj-smtr.veiculo.sppo_veiculo_dia #}
where data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")

union all

select data, id_veiculo, placa, ano_fabricacao, tecnologia, status, indicadores
from {{ ref("veiculo_dia") }}
{# from rj-smtr.monitoramento.veiculo_dia #}
where modo is null or (modo = 'ONIBUS' and tipo_veiculo not like '%ROD%')
