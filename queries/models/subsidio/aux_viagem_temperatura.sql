{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('start_date')}}") and date("{{var('end_date')}}") and data >= date("{{ var('DATA_SUBSIDIO_V15C_INICIO') }}")
{% endset %}

with
    viagens as (
        select
            data,
            servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            id_viagem,
            --ano_fabricacao
            --distancia_planejada,
            --sentido
        -- from {{ ref("viagem_classificada") }}
        FROM `rj-smtr.subsidio.viagem_classificada`
        -- where {{ incremental_filter }}
        where data = "2025-06-17"
    ),
    gps_validador as (
        select
            data,
            datetime_gps,
            datetime_captura,
            id_veiculo,
            id_validador,
            latitude,
            -- operadora,
            longitude,
            estado_equipamento,
            temperatura
        -- from {{ ref("gps_validador") }}
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`gps_validador`
        -- where {{ incremental_filter }}
        where data = "2025-06-17"
            -- and temperatura != 0
    ),
    veiculos as (
        select distinct
            id_veiculo,
            data,
            indicador_ar_condicionado,
            ano_fabricacao
        -- from {{ ref("licenciamento") }}
        from `rj-smtr`.`cadastro`.`veiculo_licenciamento_dia`
        -- where
        --     {{ incremental_filter }}
        where data = "2025-06-17"

    ),
    gps_validador_viagem as (
        select
            v.data,
            e.datetime_gps,
            e.datetime_captura,
            v.id_viagem,
            -- e.operadora,
            v.id_veiculo,
            e.id_validador,
            e.estado_equipamento,
            e.latitude,
            e.longitude,
            temperatura
        from gps_validador as e
        full join viagens as v
            on e.id_veiculo = substr(v.id_veiculo, 2)
            and e.datetime_gps between v.datetime_partida and v.datetime_chegada
    ),
    temperatura_inmet as(
        select
            id_estacao,
            temperatura,
            data_particao as data,-- atualizar dps
            horario -- atualizar dps
        from `rj-cor.clima_estacao_meteorologica.meteorologia_inmet`
        -- where {{ incremental_filter }}
        where data_particao = "2025-06-17"

    ),
    agg_temperatura_viagem as (
        select
            data,
            -- id_viagem,
            id_veiculo,
            temperatura,
            -- estado_equipamento as indicador_estado_equipamento_aberto,
            -- count(distinct temperatura) = 1) as indicador_temperatura_variacao,
            -- countif(temperatura is not null) > 0 as indicador_temperatura_transmitida,
            -- count(distinct temperatura) = 1) as indicador_temperatura_descatada,
            datetime_gps
        from gps_validador
    )
select
    v.data,
    v.id_viagem,
    v.id_veiculo,
    ve.ano_fabricacao,
    v.servico,
    ve.indicador_ar_condicionado,
    temperatura,
    datetime_gps,
    v.datetime_partida,
    v.datetime_chegada,
from viagens as v
left join veiculos as ve using (data, id_veiculo)
left join agg_temperatura_viagem as eev using (data, id_veiculo)