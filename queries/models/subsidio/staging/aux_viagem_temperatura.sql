{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('start_date')}}") and date("{{var('end_date')}}") and data >= date("{{ var('DATA_SUBSIDIO_V16_INICIO') }}")
{% endset %}

with
    viagens as (
        select
            data,
            servico_realizado as servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            id_viagem
        from {{ ref("viagem_classificada") }}
        -- from `rj-smtr.subsidio.viagem_classificada`
        where
            data between date("{{ var('start_date') }}") and date_add(
                date("{{ var('end_date') }}"), interval 1 day
            )
    -- where data = "2025-06-17"
    ),
    gps_validador as (
        select
            data,
            datetime_gps,
            servico_jae,
            id_veiculo,
            id_validador,
            estado_equipamento,
            latitude,
            longitude,
            temperatura,
            datetime_captura
        from {{ ref("gps_validador") }}
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.gps_validador`
        where
            data between date("{{ var('start_date') }}") and date_add(
                date("{{ var('end_date') }}"), interval 1 day
            )
    ),
    gps_validador_bilhetagem as (
        select
            data,
            datetime_gps,
            servico_jae,
            id_veiculo,
            id_validador,
            estado_equipamento,
            latitude,
            longitude
        from gps_validador
        where
            (
                (
                    data < date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                    and (latitude != 0 or longitude != 0)
                )
                or data >= date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
            )
            and date(datetime_captura) - date(datetime_gps) <= interval 6 day
    ),
    estado_equipamento_aux as (
        select *
        from
            (
                (
                    select
                        data,
                        servico_jae,
                        id_validador,
                        id_veiculo,
                        latitude,
                        longitude,
                        if(
                            count(case when estado_equipamento = "ABERTO" then 1 end)
                            >= 1,
                            "ABERTO",
                            "FECHADO"
                        ) as estado_equipamento,
                        min(datetime_gps) as datetime_gps,
                    from gps_validador_bilhetagem
                    where
                        (
                            data >= date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                            and latitude != 0
                            and longitude != 0
                        )
                        or data < date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                    group by 1, 2, 3, 4, 5, 6
                )
                union all
                (
                    select
                        data,
                        servico_jae,
                        id_validador,
                        id_veiculo,
                        latitude,
                        longitude,
                        estado_equipamento,
                        datetime_gps,
                    from gps_validador_bilhetagem
                    where
                        data >= date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                        and latitude = 0
                        and longitude = 0
                )
            )
    ),
    gps_validador_bilhetagem_viagem as (
        select
            v.data,
            e.datetime_gps,
            v.id_viagem,
            e.id_validador,
            e.estado_equipamento,
            e.latitude,
            e.longitude,
            v.servico,
            e.servico_jae,
        from viagens as v
        left join
            estado_equipamento_aux as e
            on e.id_veiculo = substr(v.id_veiculo, 2)
            and e.datetime_gps between v.datetime_partida and v.datetime_chegada
    ),
    indicador_equipamento_bilhetagem as (
        select
            data,
            id_viagem,
            id_validador,
            countif(servico != servico_jae) > 0 as indicador_gps_servico_divergente,
            countif(estado_equipamento = "ABERTO") / count(*)
            >= 0.8 as indicador_estado_equipamento_aberto
        from gps_validador_bilhetagem_viagem
        group by all
    ),
    indicadores_temperatura_veiculo as (
        select
            data,
            id_veiculo,
            countif(temperatura is not null) > 0 as indicador_temperatura_transmitida,
            count(distinct temperatura) = 1 as indicador_temperatura_variacao,
        from gps_validador
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
        group by 1, 2
    ),
    veiculos as (
        select distinct id_veiculo, data, indicador_ar_condicionado, ano_fabricacao
        -- from {{ ref("licenciamento") }}
        from `rj-smtr`.`cadastro`.`veiculo_licenciamento_dia`
        -- where
        -- {{ incremental_filter }}
        where data = "2025-06-17"

    ),
    gps_validador_viagem as (
        select
            v.data,
            e.datetime_gps,
            e.datetime_captura,
            v.id_viagem,
            v.id_veiculo,
            e.id_validador,
            e.estado_equipamento,
            e.latitude,
            e.longitude,
            temperatura
        from gps_validador as e
        full join
            viagens as v
            on e.id_veiculo = substr(v.id_veiculo, 2)
            and e.datetime_gps between v.datetime_partida and v.datetime_chegada
    ),
    temperatura_inmet as (
        select
            id_estacao,
            temperatura,
            data_particao as data,  -- atualizar dps
            horario  -- atualizar dps
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
            -- countif(temperatura is not null) > 0 as
            -- indicador_temperatura_transmitida,
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
