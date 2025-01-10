{{ config(materialized="view") }}

select
    do.modo,
    g.data,
    g.hora,
    g.data_tracking as datetime_gps,
    g.timestamp_captura as datetime_captura,
    do.id_operadora,
    do.operadora,
    g.codigo_linha_veiculo as id_servico_jae,
    -- s.servico,
    l.nr_linha as servico_jae,
    l.nm_linha as descricao_servico_jae,
    prefixo_veiculo as id_veiculo,
    g.numero_serie_equipamento as id_validador,
    g.id as id_transmissao_gps,
    g.latitude_equipamento as latitude,
    g.longitude_equipamento as longitude,
    initcap(g.sentido_linha) as sentido,
    g.estado_equipamento,
    g.temperatura,
    g.versao_app
from {{ ref("staging_gps_validador") }} g
left join {{ ref("operadoras") }} as do on g.codigo_operadora = do.id_operadora_jae
left join
    {{ ref("staging_linha") }} as l on g.codigo_linha_veiculo = l.cd_linha
    -- LEFT JOIN
    -- {{ ref("servicos") }} AS s
    -- ON
    -- g.codigo_linha_veiculo = s.id_servico_jae

