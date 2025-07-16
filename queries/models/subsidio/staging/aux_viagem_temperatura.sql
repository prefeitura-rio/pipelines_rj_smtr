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
            servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            id_viagem,
            --ano_fabricacao,
            -- distancia_planejada,
            -- sentido,
            -- modo,
        -- from {{ ref("viagem_classificada") }}
        from `rj-smtr.subsidio.viagem_classificada`
        -- where
        --     data between date("{{ var('start_date') }}") and date_add(
        --         date("{{ var('end_date') }}"), interval 1 day
            -- )
        where data = "2025-06-17"
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
        -- from {{ ref("gps_validador") }}
        from `rj-smtr.br_rj_riodejaneiro_bilhetagem.gps_validador`
        -- where
        --     data between date("{{ var('start_date') }}") and date_add(
        --         date("{{ var('end_date') }}"), interval 1 day
        --     )
            where data = "2025-06-17"
    ),
    indicadores_temperatura_veiculo as (
        select
            data,
            id_veiculo,
            countif(temperatura is not null) > 0 as indicador_temperatura_transmitida,
            count(distinct temperatura) = 1 as indicador_temperatura_variacao,
        from gps_validador
        -- where
        --     data
        --     between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
        where data = "2025-06-17"
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
            temperatura,
            EXTRACT(HOUR FROM datetime_gps) AS hora,
        from viagens as v
        left join gps_validador as e
            on e.id_veiculo = substr(v.id_veiculo, 2)
            and e.datetime_gps between v.datetime_partida and v.datetime_chegada
    ),
    gps_validador_indicadores as(
        select
            data,
            id_veiculo,
            count(temperatura) as quantidade_pre_tratamento,
            countif(temperatura is null or temperatura = 0) as quantidade_nula_zero
        from gps_validador_viagem
        group by 1,2
    ),
    temperatura_inmet as (
        select
            MAX(temperatura) as temperatura,
            data_particao as data,  -- atualizar dps
            EXTRACT(HOUR FROM horario) as hora  -- atualizar dps
        from `rj-cor.clima_estacao_meteorologica.meteorologia_inmet`
        -- where {{ incremental_filter }}
        where data_particao = "2025-06-17"
        and id_estacao in("A621", "A652", "A636", "A602")
        group by all
    ),
    metricas_base as (
        select
            id_veiculo,
            data,
            hora,
            temperatura,
            -- Cálcula 1 e 3 quartil
            percentile_cont(temperatura, 0.25) over (partition by data, hora) as q1,
            percentile_cont(temperatura, 0.75) over (partition by data, hora) as q3,
            -- Cálcula a mediana
            percentile_cont(temperatura, 0.5) over (partition by data, hora) as mediana
        from gps_validador_viagem
        where temperatura is not null or temperatura != 0
    ),
    metricas_estatisticas as (
        select
            *,
            -- Cálcula o intervalo do IQR
            q3 - q1 as iqr,
            -- Cálcula os limites inferior e superior do IQR
            q1 - 1.5 * (q3 - q1) as iqr_limite_inferior,
            q3 + 1.5 * (q3 - q1) as iqr_limite_superior,
            -- Desvio absoluto da mediana
            abs(temperatura - mediana) as desvio_abs
        from metricas_base
    ),
    metrica_mad as (
        select
            *,
            -- MAD = mediana dos desvios absolutos
            percentile_cont(desvio_abs, 0.5) over (partition by data, hora) as mad
        from metricas_estatisticas
    ),
    metrica_robust_z_score as (
        select
            *,
            -- Modified Z-Score
            safe_divide(0.6745 * (temperatura - mediana), mad) as robust_z_score
        from metrica_mad
    ),
    temperatura_filtrada as (
        select
        *,
        COUNT(*) quantidade_pos_tratamento
        from metrica_robust_z_score
        where
            temperatura > iqr_limite_inferior
            or temperatura < iqr_limite_superior
            and robust_z_score > 3.5
        group by all
    ),
    agg_temperatura_viagem as ( -- TESTAR SUM EM TUDO E SEPARADAMENTE
        select
            data,
            id_veiculo,
            SAFE_DIVIDE(SUM(quantidade_pos_tratamento), SUM(quantidade_pre_tratamento - quantidade_nula_zero)) * 100 as indicador_temperatura_atipica_descartada,
            SAFE_DIVIDE(SUM(quantidade_nula_zero), SUM(quantidade_pre_tratamento)) * 100 as indicador_temperatura_nula_descartada
        from gps_validador_indicadores
        left join temperatura_filtrada
            using(data, id_veiculo)
        group by 1,2
    ),
    classificacao_temperatura as (
        select
            data,
            hora,
            id_veiculo,
            id_viagem,
            g.temperatura as temperatura_int,
            t.temperatura as temperatura_ext,
            g.temperatura <= 24 or ((t.temperatura - g.temperatura) > 8) as classificacao_temperatura_regular
        from gps_validador_viagem as g
        left join temperatura_inmet as t using(data, hora)
    ),
    percentual_viagem as (
        select
        id_viagem,
        (countif(classificacao_temperatura_regular) / count(*)) * 100 as percentual_temperatura_regular
        from classificacao_temperatura
        group by 1
    ) select * from percentual_viagem

