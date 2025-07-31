{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date_add(date("{{ var('date_range_end') }}"), interval 1 day) and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

with
    viagens as (  -- Viagens realizadas no período de apuração
        select
            data,
            servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            placa,
            ano_fabricacao,
            id_viagem,
            tipo_viagem,
            indicadores,
            safe_cast(
                json_value(indicadores, '$.indicador_ar_condicionado.valor') as bool
            ) as indicador_ar_condicionado,
            distancia_planejada,
            tecnologia_apurada,
            tecnologia_remunerada,
            sentido,
            modo
        from {{ ref("viagem_classificada") }}
        {# from `rj-smtr-dev.botelho__subsidio.viagem_classificada` #}
        where
            data between date("{{var('date_range_start')}}") and date(
                "{{var('date_range_end')}}"
            )
            and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    ),
    gps_validador as (  -- Dados base de GPS, temperatura, etc
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
    estado_equipamento_aux as (  -- Dados de estado do equipamento do validador
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
                    from gps_validador
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
                    from gps_validador
                    where latitude = 0 and longitude = 0
                )
            )
    ),
    gps_validador_bilhetagem_viagem as (  -- Dados de bilhetagem com base apenas nas viagens realizadas
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
    indicador_equipamento_bilhetagem as (  -- Indicadores de estado do equipamento do validador por viagem
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
    gps_validador_viagem as (  -- Dados completos de GPS, temperatura e bilhetagem por viagem realizada
        select
            v.data,
            e.datetime_gps,
            v.datetime_partida,
            v.datetime_chegada,
            e.datetime_captura,
            v.id_viagem,
            v.id_veiculo,
            v.indicador_ar_condicionado,
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
    gps_validador_indicadores as (  -- Indicadores de temperatura por veículo
        select
            data,
            id_veiculo,
            count(*) as quantidade_pre_tratamento,
            countif(temperatura is null or temperatura = 0) as quantidade_nula_zero,
            countif(temperatura is not null) > 0 as indicador_temperatura_transmitida,
            count(distinct temperatura) > 1 as indicador_temperatura_variacao
        from gps_validador_viagem
        where indicador_ar_condicionado
        group by 1, 2
    ),
    temperatura_inmet as (  -- Dados de temperatura externa do INMET
        select data, extract(hour from hora) as hora, max(temperatura) as temperatura
        from {{ ref("temperatura_inmet") }}
        where
            {{ incremental_filter }} and id_estacao in ("A621", "A652", "A636", "A602")  -- Estações do Rio de Janeiro
        group by 1, 2
    ),
    metricas_base as (  -- 1 e 3 quartil da temperatura por hora e dia
        select
            g.id_veiculo,
            g.data,
            hora,
            temperatura,
            datetime_gps,
            percentile_cont(temperatura, 0.25) over (partition by g.data, hora) as q1,
            percentile_cont(temperatura, 0.75) over (partition by g.data, hora) as q3
        from gps_validador_viagem as g
        where temperatura is not null and temperatura != 0 and indicador_ar_condicionado
    ),
    metricas_iqr as (  -- IQR, limites inferior e superior
        select
            *,
            q3 - q1 as iqr,
            q1 - 1.5 * (q3 - q1) as iqr_limite_inferior,
            q3 + 1.5 * (q3 - q1) as iqr_limite_superior
        from metricas_base
    ),
    temperatura_filtrada_iqr as (  -- Filtro dos dados atípicos com IQR e calcula métricas base para Robust Z-Score
        select
            *,
            count(*) quantidade_pos_tratamento_iqr,
            percentile_cont(temperatura, 0.5) over (partition by data, hora) as mediana
        from metricas_iqr
        where temperatura >= iqr_limite_inferior and temperatura <= iqr_limite_superior
        group by all
    ),
    metrica_mediana as (  -- Métrica base para Robust Z-Score - Desvio Absoluto
        select *, abs(temperatura - mediana) as desvio_abs from temperatura_filtrada_iqr
    ),
    metrica_mad as (  -- Métrica base para Robust Z-Score - MAD
        select *, percentile_cont(desvio_abs, 0.5) over (partition by data, hora) as mad
        from metrica_mediana
    ),
    metrica_robust_z_score as (  -- Robust Z-Score
        select *, safe_divide(0.6745 * (temperatura - mediana), mad) as robust_z_score
        from metrica_mad
    ),
    temperatura_filtrada_total as (  -- Filtro dos dados atípicos com base no Robust Z-Score
        select *, count(*) as quantidade_pos_tratamento_total
        from metrica_robust_z_score
        where abs(robust_z_score) <= 3.5
        group by all
    ),
    agg_temperatura_viagem as (  -- Percentuais de temperatura descartada e nula
        select
            data,
            id_veiculo,
            quantidade_pre_tratamento,
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
    classificacao_temperatura as (  -- Regras para classificação de temperatura regular
        select
            i.data,
            i.hora,
            i.id_veiculo,
            id_viagem,
            indicador_ar_condicionado,
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
    percentual_indicadores_viagem as (  -- Indicadores de regularidade de temperatura
        select
            data,
            id_veiculo,
            id_viagem,
            quantidade_pre_tratamento,
            case
                when max(indicador_ar_condicionado)
                then trunc((countif(classificacao_temperatura_regular) / count(*)), 2)
                else null
            end as percentual_temperatura_regular,
            case
                when max(indicador_ar_condicionado)
                then (countif(classificacao_temperatura_regular) / count(*)) >= 0.8
                else null
            end as indicador_temperatura_regular,
            case
                when max(indicador_ar_condicionado)
                then current_datetime("America/Sao_Paulo")
                else null
            end as datetime_apuracao_subsidio,
            indicador_temperatura_variacao,
            indicador_temperatura_transmitida,
            percentual_temperatura_nula_descartada,
            percentual_temperatura_atipica_descartada,
            (
                percentual_temperatura_nula_descartada
                + percentual_temperatura_atipica_descartada
            )
            > 0.5 as indicador_temperatura_descartada
        from classificacao_temperatura
        left join agg_temperatura_viagem using (data, id_veiculo)
        group by all
    ),
    struct_indicadores as (  -- Estrutura indicadores em formato JSON
        select
            v.data,
            v.id_viagem,
            v.id_veiculo,
            v.placa,
            v.ano_fabricacao,
            v.tipo_viagem,
            to_json_string(v.indicadores) as indicadores_str,
            v.datetime_partida,
            v.datetime_chegada,
            v.modo,
            v.servico,
            v.sentido,
            v.distancia_planejada,
            v.tecnologia_apurada,
            v.tecnologia_remunerada,
            to_json_string(
                struct(
                    struct(
                        p.datetime_apuracao_subsidio,
                        p.indicador_temperatura_regular as valor,
                        safe_cast(
                            p.percentual_temperatura_regular as string
                        ) as percentual_temperatura_regular
                    ) as indicador_temperatura_regular,
                    struct(
                        p.datetime_apuracao_subsidio,
                        p.indicador_temperatura_variacao as valor
                    ) as indicador_temperatura_variacao,
                    struct(
                        p.datetime_apuracao_subsidio,
                        p.indicador_temperatura_transmitida as valor
                    ) as indicador_temperatura_transmitida,
                    struct(
                        p.datetime_apuracao_subsidio,
                        p.indicador_temperatura_descartada as valor,
                        safe_cast(
                            p.percentual_temperatura_nula_descartada as string
                        ) as percentual_temperatura_nula_descartada,
                        safe_cast(
                            p.percentual_temperatura_atipica_descartada as string
                        ) as percentual_temperatura_atipica_descartada
                    ) as indicador_temperatura_descartada,
                    struct(
                        current_datetime(
                            "America/Sao_Paulo"
                        ) as datetime_apuracao_subsidio,
                        (
                            select
                                array_agg(
                                    struct(
                                        ieb.id_validador,
                                        ieb.indicador_gps_servico_divergente,
                                        ieb.indicador_estado_equipamento_aberto,
                                        safe_cast(
                                            ieb.percentual_estado_equipamento_aberto
                                            as string
                                        ) as percentual_estado_equipamento_aberto
                                    )
                                )
                            from indicador_equipamento_bilhetagem ieb
                            where ieb.data = v.data and ieb.id_viagem = v.id_viagem
                        ) as valores
                    ) as indicador_validador
                )
            ) as indicadores_novos,
            p.quantidade_pre_tratamento
        from viagens as v
        left join percentual_indicadores_viagem as p using (data, id_viagem)
    )
select  -- Estrutura final do modelo auxiliar
    s.data,
    s.id_viagem,
    s.id_veiculo,
    s.placa,
    s.ano_fabricacao,
    s.datetime_partida,
    s.datetime_chegada,
    s.modo,
    s.tecnologia_apurada,
    s.tecnologia_remunerada,
    s.tipo_viagem,
    parse_json(
        concat(
            left(s.indicadores_str, length(s.indicadores_str) - 1),
            ',',
            substr(s.indicadores_novos, 2)
        )
    ) as indicadores,
    s.servico,
    s.sentido,
    s.distancia_planejada,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from struct_indicadores as s
