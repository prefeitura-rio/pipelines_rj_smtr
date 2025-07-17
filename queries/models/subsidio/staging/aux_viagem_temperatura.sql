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
            id_viagem,
        -- ano_fabricacao,
        -- distancia_planejada,
        -- sentido,
        -- modo,
        -- from {{ ref("viagem_classificada") }}
        from `rj-smtr.projeto_subsidio_sppo.viagem_completa`
        -- where
        -- data between date("{{ var('start_date') }}") and date_add(
        -- date("{{ var('end_date') }}"), interval 1 day
        -- )
        where data = "2025-05-12"
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
            safe_cast(temperatura as numeric) as temperatura,
            datetime_captura
        -- from {{ ref("gps_validador") }}
        from `rj-smtr.br_rj_riodejaneiro_bilhetagem.gps_validador`
        -- where
        -- data between date("{{ var('start_date') }}") and date_add(
        -- date("{{ var('end_date') }}"), interval 1 day
        -- )
        where data = "2025-05-12"
    ),
    veiculos as (
        select distinct id_veiculo, data, indicador_ar_condicionado, ano_fabricacao
        -- from {{ ref("licenciamento") }}
        from `rj-smtr`.`cadastro`.`veiculo_licenciamento_dia`
        -- where
        -- {{ incremental_filter }}
        where data = "2025-05-12"
    ),
    gps_validador_viagem as (
        select
            v.data,
            e.datetime_gps,
            v.datetime_partida,
            v.datetime_chegada,
            e.datetime_captura,
            v.id_viagem,
            v.id_veiculo,
            e.id_validador,
            e.estado_equipamento,
            e.latitude,
            e.longitude,
            temperatura,
            extract(hour from datetime_gps) as hora,
        from viagens as v
        left join
            gps_validador as e
            on e.id_veiculo = substr(v.id_veiculo, 2)
            and e.datetime_gps between v.datetime_partida and v.datetime_chegada
    ),
    gps_validador_indicadores as (
        select
            data,
            id_veiculo,
            count(*) as quantidade_pre_tratamento,
            countif(temperatura is null or temperatura = 0) as quantidade_nula_zero,
            countif(temperatura is not null) > 0 as indicador_temperatura_transmitida,
            count(distinct temperatura) > 1 as indicador_temperatura_variacao,
        from gps_validador_viagem
        group by 1, 2
    ),
    temperatura_inmet as (
        select
            max(temperatura) as temperatura,
            data_particao as data,  -- atualizar dps
            extract(hour from horario) as hora  -- atualizar dps
        from `rj-cor.clima_estacao_meteorologica.meteorologia_inmet`
        -- where {{ incremental_filter }}
        where
            data_particao = "2025-05-12"
            and id_estacao in ("A621", "A652", "A636", "A602")
        group by all
    ),
    metricas_base as (
        select
            g.id_veiculo,
            g.data,
            hora,
            temperatura,
            datetime_gps,
            -- Cálcula 1 e 3 quartil
            percentile_cont(temperatura, 0.25) over (partition by g.data, hora) as q1,
            percentile_cont(temperatura, 0.75) over (partition by g.data, hora) as q3,
        from gps_validador_viagem as g
        left join veiculos as ve using (id_veiculo)
        where temperatura is not null and temperatura != 0
    ),
    metricas_iqr as (
        select
            *,
            -- Cálcula o intervalo do IQR
            q3 - q1 as iqr,
            -- Cálcula os limites inferior e superior do IQR
            q1 - 1.5 * (q3 - q1) as iqr_limite_inferior,
            q3 + 1.5 * (q3 - q1) as iqr_limite_superior
        from metricas_base
    ),
    temperatura_filtrada_iqr as (
        select
            *,
            count(*) quantidade_pos_tratamento_iqr,
            -- Cálcula a mediana para usar no calculo do robust z-score
            percentile_cont(temperatura, 0.5) over (partition by data, hora) as mediana
        from metricas_iqr
        where  -- Filtra os dados com iqr
            temperatura >= iqr_limite_inferior and temperatura <= iqr_limite_superior
        group by all
    ),
    metrica_mediana as (
        select
            *,
            -- Desvio absoluto da mediana para usar no calculo do robust z-score
            abs(temperatura - mediana) as desvio_abs
        from temperatura_filtrada_iqr
    ),
    metrica_mad as (
        select
            *,
            -- MAD = mediana dos desvios absolutos para usar no calculo do robust
            -- z-score
            percentile_cont(desvio_abs, 0.5) over (partition by data, hora) as mad
        from metrica_mediana
    ),
    metrica_robust_z_score as (
        select
            *,
            -- Robust Z-Score
            safe_divide(0.6745 * (temperatura - mediana), mad) as robust_z_score
        from metrica_mad
    ),
    temperatura_filtrada_total as (
        select *, count(*) quantidade_pos_tratamento_total
        from metrica_robust_z_score
        where  -- Filtra os dados com o r z-score
            abs(robust_z_score) <= 3.5
        group by all
    ),
    agg_temperatura_viagem as (
        select
            data,
            id_veiculo,
            trunc(
                coalesce(
                    safe_divide(
                        sum(quantidade_pos_tratamento_total),
                        sum(quantidade_pre_tratamento)
                    )
                    * 100,
                    0
                ),
                2
            ) as percentual_temperatura_atipica_descartada,
            trunc(
                coalesce(
                    safe_divide(
                        sum(quantidade_nula_zero), sum(quantidade_pre_tratamento)
                    )
                    * 100,
                    0
                ),
                2
            ) as percentual_temperatura_nula_descartada,
            indicador_temperatura_transmitida,
            indicador_temperatura_variacao,
        from gps_validador_indicadores
        left join temperatura_filtrada_total using (data, id_veiculo)
        group by all
    ),
    classificacao_temperatura as (
        select
            i.data,
            i.hora,
            i.id_veiculo,
            id_viagem,
            i.datetime_gps,
            f.temperatura as temperatura_int,
            e.temperatura as temperatura_ext,
            f.temperatura <= 24
            or (
                (e.temperatura - f.temperatura) > 8
            ) as classificacao_temperatura_regular,
            f.temperatura <= 24 as indicador_temperatura_menor_igual_24,
            (
                (e.temperatura - f.temperatura) > 8
            ) as indicador_diferenca_temperatura_externa_interna
        from gps_validador_viagem as i
        left join
            temperatura_filtrada_total as f
            on f.data = i.data
            and f.datetime_gps between datetime_partida and datetime_chegada
            and f.id_veiculo = i.id_veiculo
        left join temperatura_inmet as e on e.data = i.data and e.hora = i.hora
    ),
    percentual_indicadores_viagem as (
        select
            data,
            id_veiculo,
            id_viagem,
            trunc(
                (countif(classificacao_temperatura_regular) / count(*)) * 100, 2
            ) as percentual_temperatura_regular,
            (countif(classificacao_temperatura_regular) / count(*)) * 100
            >= 80 as indicador_temperatura_regular,
            indicador_temperatura_variacao,
            indicador_temperatura_transmitida,
            percentual_temperatura_nula_descartada,
            percentual_temperatura_atipica_descartada,
            (
                percentual_temperatura_nula_descartada
                + percentual_temperatura_atipica_descartada
            ) as teste,
            (
                percentual_temperatura_nula_descartada
                + percentual_temperatura_atipica_descartada
            )
            > 50 as indicador_temperatura_descartada,
        from classificacao_temperatura
        left join agg_temperatura_viagem using (data, id_veiculo)
        group by all
    )
select *
from percentual_indicadores_viagem
