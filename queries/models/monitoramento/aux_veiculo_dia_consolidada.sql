{{
    config(
        materialized="ephemeral",
    )
}}

select
    data,
    id_veiculo,
    placa,
    ano_fabricacao,
    tecnologia,
    if(tipo_veiculo like '%ROD%', "Não licenciado", status) as status,
    indicadores
from `rj-smtr-dev.janaina__reprocessamento__monitoramento.veiculo_dia`
where modo is null or modo = 'ONIBUS'
