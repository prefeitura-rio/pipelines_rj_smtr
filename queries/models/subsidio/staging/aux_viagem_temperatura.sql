{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('start_date')}}") and date_add(date("{{ var('end_date') }}"), interval 1 day) and data >= date("{{ var('DATA_SUBSIDIO_V16_INICIO') }}")
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
            tipo_viagem,
            indicadores,
            ano_fabricacao,
            distancia_planejada,
            sentido,
            modo
        from {{ ref("viagem_classificada") }}
        where
            data between date("{{var('start_date')}}") and date("{{var('end_date')}}")
            and data >= date("{{ var('DATA_SUBSIDIO_V16_INICIO') }}")
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
        from {{ ref("gps_validador") }}
        where {{ incremental_filter }}
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
        where date_diff(date(datetime_captura), date(datetime_gps), day) <= 6
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
                    where latitude != 0 and longitude != 0
                    group by all
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
                        datetime_gps
                    from gps_validador_bilhetagem
                    where latitude = 0 and longitude = 0
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
            e.servico_jae
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
            trunc(
                countif(estado_equipamento = "ABERTO") / count(*), 5
            ) as percentual_estado_equipamento_aberto,
            countif(estado_equipamento = "ABERTO") / count(*)
            >= 0.8 as indicador_estado_equipamento_aberto
        from gps_validador_bilhetagem_viagem
        group by 1, 2, 3
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
            extract(hour from datetime_gps) as hora
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
            count(distinct temperatura) > 1 as indicador_temperatura_variacao
        from gps_validador_viagem
        group by 1, 2
    ),
    temperatura_inmet as (
        select data, extract(hour from hora) as hora, max(temperatura) as temperatura
        from {{ ref("temperatura_inmet") }}
        where
            {{ incremental_filter }} and id_estacao in ("A621", "A652", "A636", "A602")  -- Estações do Rio de Janeiro
        group by 1, 2
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
        where temperatura is not null and temperatura != 0
    ),
    metricas_iqr as (
        select
            *,
            q3 - q1 as iqr,  -- Cálcula o intervalo do IQR
            q1 - 1.5 * (q3 - q1) as iqr_limite_inferior,  -- Cálcula os limites inferior e superior do IQR
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
            -- Mediana dos desvios absolutos para usar no calculo do robust z-score
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
        select *, count(*) as quantidade_pos_tratamento_total
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
                    ),
                    0
                ),
                2
            ) as percentual_temperatura_atipica_descartada,
            trunc(
                coalesce(
                    safe_divide(
                        sum(quantidade_nula_zero), sum(quantidade_pre_tratamento)
                    ),
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
        left join
            temperatura_inmet as e
            on e.data = extract(date from i.datetime_gps)
            and e.hora = extract(hour from i.datetime_gps)
    ),
    percentual_indicadores_viagem as (
        select
            data,
            id_veiculo,
            id_viagem,
            trunc(
                (countif(classificacao_temperatura_regular) / count(*)), 2
            ) as percentual_temperatura_regular,
            (countif(classificacao_temperatura_regular) / count(*))
            >= 0.8 as indicador_temperatura_regular,
            indicador_temperatura_variacao,
            indicador_temperatura_transmitida,
            percentual_temperatura_nula_descartada,
            percentual_temperatura_atipica_descartada,
            (
                percentual_temperatura_nula_descartada
                + percentual_temperatura_atipica_descartada
            )
            > 0.5 as indicador_temperatura_descartada,
        from classificacao_temperatura
        left join agg_temperatura_viagem using (data, id_veiculo)
        group by all
    ),
    struct_indicadores as (
        select
            v.data,
            v.id_viagem,
            v.id_veiculo,
            v.tipo_viagem,
            to_json_string(v.indicadores) as indicadores_str,
            v.datetime_partida,
            v.datetime_chegada,
            v.modo,
            v.servico,
            v.sentido,
            v.distancia_planejada,
            v.ano_fabricacao,
            to_json_string(
                struct(
                    struct(
                        current_date("America/Sao_Paulo") as data_apuracao_subsidio,
                        p.indicador_temperatura_regular as valor,
                        p.percentual_temperatura_regular
                        as percentual_temperatura_regular
                    ) as indicador_temperatura_regular,
                    struct(
                        current_date("America/Sao_Paulo") as data_apuracao_subsidio,
                        p.indicador_temperatura_variacao as valor
                    ) as indicador_temperatura_variacao,
                    struct(
                        current_date("America/Sao_Paulo") as data_apuracao_subsidio,
                        p.indicador_temperatura_transmitida as valor
                    ) as indicador_temperatura_transmitida,
                    struct(
                        current_date("America/Sao_Paulo") as data_apuracao_subsidio,
                        p.indicador_temperatura_descartada as valor,
                        p.percentual_temperatura_nula_descartada
                        as percentual_temperatura_nula_descartada,
                        p.percentual_temperatura_atipica_descartada
                        as percentual_temperatura_atipica_descartada
                    ) as indicador_temperatura_descartada,
                    struct(
                        current_date("America/Sao_Paulo") as data_apuracao_subsidio,
                        (
                            select
                                array_agg(
                                    struct(
                                        ieb.id_validador,
                                        ieb.indicador_gps_servico_divergente,
                                        ieb.indicador_estado_equipamento_aberto,
                                        ieb.percentual_estado_equipamento_aberto
                                    )
                                )
                            from indicador_equipamento_bilhetagem ieb
                            where ieb.data = v.data and ieb.id_viagem = v.id_viagem
                        ) as valores
                    ) as indicador_validador
                )
            ) as indicadores_novos
        from viagens as v
        left join percentual_indicadores_viagem as p using (data, id_viagem)
    )
select
    s.data,
    s.id_viagem,
    s.id_veiculo,
    s.tipo_viagem,
    parse_json(
        concat(
            left(s.indicadores_str, length(s.indicadores_str) - 1),
            ',',
            substr(s.indicadores_novos, 2)
        )
    ) as indicadores,
    s.datetime_partida,
    s.datetime_chegada,
    s.modo,
    s.servico,
    s.sentido,
    s.distancia_planejada,
    s.ano_fabricacao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from struct_indicadores as s
