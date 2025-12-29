{{
    config(
        materialized="ephemeral",
    )
}}


select data, id_veiculo, placa, ano_fabricacao, tecnologia, status, indicadores
from `rj-smtr-dev.janaina__reprocessamento__monitoramento.veiculo_dia`
where modo is null or (modo = 'ONIBUS' and tipo_veiculo not like '%ROD%')
