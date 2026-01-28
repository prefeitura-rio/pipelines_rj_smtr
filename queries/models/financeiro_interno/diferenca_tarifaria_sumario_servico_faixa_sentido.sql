{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    percentual_operacao as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            km_planejada_faixa,
            consorcio,
            servico,
            sentido,
            pof
        from {{ ref("percentual_operacao_faixa_horaria") }}
        -- from `rj-smtr.subsidio.percentual_operacao_faixa_horaria`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    viagens as (
        select
            data,
            datetime_partida,
            servico,
            sentido,
            id_viagem,
            safe_cast(distancia_planejada as numeric) as distancia_planejada,
            receita_tarifa_publica,
            irk,
            indicador_viagem_dentro_limite,
            indicador_conformidade,
            indicador_validade
        from {{ ref("viagens_remuneradas") }}
        {# from `rj-smtr-dev`.`rodrigo__dashboard_subsidio_sppo`.`viagens_remuneradas` #}
        -- `rj-smtr.dashboard_subsidio_sppo.viagens_remuneradas`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    penalidade as (
        select
            data,
            tipo_dia,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            valor_penalidade
        from {{ ref("subsidio_penalidade_servico_faixa") }}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    subsidio_servico as (
        select
            p.data,
            p.tipo_dia,
            p.faixa_horaria_inicio,
            p.faixa_horaria_fim,
            p.consorcio,
            p.servico,
            p.sentido,
            p.pof,
            any_value(irk) over (partition by p.data) as irk,
            v.id_viagem,
            receita_tarifa_publica,
            p.km_planejada_faixa,
            distancia_planejada,
            v.indicador_viagem_dentro_limite,
            indicador_conformidade,
            indicador_validade
        from percentual_operacao as p
        left join
            viagens as v
            on p.data = v.data
            and p.servico = v.servico
            and p.sentido = v.sentido
            and v.datetime_partida
            between p.faixa_horaria_inicio and p.faixa_horaria_fim
    ),
    subsidio_km as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            sentido,
            pof,
            irk,
            count(*) as viagens_faixa,
            any_value(km_planejada_faixa) as km_planejada_faixa,
            sum(
                if(
                    indicador_conformidade and indicador_viagem_dentro_limite,
                    distancia_planejada,
                    0
                )
            ) as km_conforme_faixa,
            sum(if(indicador_validade, distancia_planejada, 0)) as km_atendida_faixa,
            coalesce(sum(receita_tarifa_publica), 0) as receita_tarifa_publica_faixa,
        from subsidio_servico
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
select
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    sentido,
    viagens_faixa,
    pof as percentual_atendimento,
    irk,
    km_planejada_faixa,
    km_conforme_faixa,
    km_atendida_faixa,
    receita_tarifa_publica_faixa,
    km_conforme_faixa * irk as receita_irk_faixa,
    valor_penalidade,
    -- Cenário A: Cenário Base
    if(
        pof >= 80,
        km_conforme_faixa * irk - receita_tarifa_publica_faixa,
        least((km_planejada_faixa * irk) - receita_tarifa_publica_faixa, 0)
    )
    + valor_penalidade as delta_tr_a,
    -- Cenário B: Cenário A, mas quando o POF < 80, calcula-se o delta considerando
    -- 80% da km_planejada_faixa (o mínimo que ele deveria cumprir)
    if(
        pof >= 80,
        km_conforme_faixa * irk - receita_tarifa_publica_faixa,
        least((km_planejada_faixa * irk * 0.8) - receita_tarifa_publica_faixa, 0)
    )
    + valor_penalidade as delta_tr_b,
    -- Cenário C: Cenário A, mas quando o POF < 80 e S < 0, calcula-se o delta
    -- abatendo o valor_penalidade no saldo negativo
    case
        /*
        S = (km_conforme_faixa * irk) - receita_tarifa_publica_faixa
        P = valor_penalidade
    */
        -- Regime normal: pode haver pagamento ao operador
        when pof >= 80
        then (km_conforme_faixa * irk) - receita_tarifa_publica_faixa

        -- POF < 80
        else
            case
                -- Se S >= 0, subsídio não entra no encontro: Δ = P
                when (km_planejada_faixa * irk) - receita_tarifa_publica_faixa >= 0
                then valor_penalidade

                -- Se S < 0, saldo negativo compensa a penalidade
                else
                    - abs(
                        abs((km_planejada_faixa * irk) - receita_tarifa_publica_faixa)
                        - abs(valor_penalidade)
                    )
            end
    end as delta_tr_c,
    -- Cenário D: Cenário A, mas quando o POF < 80, Cenário B quando e S < 0,
    -- calcula-se o delta abatendo o valor_penalidade no saldo negativo (a mesma
    -- lógica do C, mas com 80% da km_planejada_faixa)
    case
        -- Regime normal: pode haver pagamento ao operador
        when pof >= 80
        then (km_conforme_faixa * irk) - receita_tarifa_publica_faixa

        -- POF < 80
        else
            case
                -- Se S >= 0, subsídio não entra no encontro: Δ = P
                when
                    (km_planejada_faixa * irk * 0.8) - receita_tarifa_publica_faixa >= 0
                then valor_penalidade

                -- Se S < 0, saldo negativo compensa a penalidade
                else
                    - abs(
                        abs(
                            (km_planejada_faixa * irk * 0.8)
                            - receita_tarifa_publica_faixa
                        )
                        - abs(valor_penalidade)
                    )
            end
    end as delta_tr_d,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from subsidio_km
left join
    penalidade using (
        data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, servico, sentido
    )
