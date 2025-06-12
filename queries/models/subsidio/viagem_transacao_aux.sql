{{ config(materialized="ephemeral") }}

{% if var("tipo_materializacao") == "monitoramento" %} {% set interval_minutes = 120 %}
{% elif var("tipo_materializacao") == "subsidio" %} {% set interval_minutes = 30 %}
{% endif %}
with
    -- 1. Transações Jaé
    transacao as (
        select id_veiculo, servico_jae, datetime_transacao
        from {{ ref("transacao") }}
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao`
        where
            data between date("{{ var('start_date') }}") and date_add(
                date("{{ var('end_date') }}"), interval 1 day
            )
            and date(datetime_processamento) - date(datetime_transacao)
            <= interval 6 day
    ),
    -- 2. Transações RioCard
    transacao_riocard as (
        select id_veiculo, servico_jae, datetime_transacao
        from {{ ref("transacao_riocard") }}
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao_riocard`
        where
            data between date("{{ var('start_date') }}") and date_add(
                date("{{ var('end_date') }}"), interval 1 day
            )
            and date(datetime_processamento) - date(datetime_transacao)
            <= interval 6 day
    ),
    -- 3. GPS Validador
    gps_validador as (
        select
            data,
            datetime_gps,
            servico_jae,
            id_veiculo,
            id_validador,
            estado_equipamento,
            latitude,
            longitude
        from {{ ref("gps_validador") }}
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.gps_validador`
        where
            data between date("{{ var('start_date') }}") and date_add(
                date("{{ var('end_date') }}"), interval 1 day
            )
            and (
                (
                    data < date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                    and (latitude != 0 or longitude != 0)
                )
                or data >= date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
            )
            and date(datetime_captura) - date(datetime_gps) <= interval 6 day
    ),
    -- 4. Viagens realizadas
    viagem as (
        select
            data,
            servico_realizado as servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            id_viagem,
            distancia_planejada,
            sentido
        from {{ ref("viagem_completa") }}
        -- from `rj-smtr.projeto_subsidio_sppo.viagem_completa`
        where
            data
            between date_sub(date("{{ var('start_date') }}"), interval 1 day) and date(
                "{{ var('end_date') }}"
            )
    ),
    -- 5. Status dos veículos
    veiculos as (
        select data, id_veiculo, status, tecnologia
        from {{ ref("sppo_veiculo_dia") }}
        -- from `rj-smtr.veiculo.sppo_veiculo_dia`
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
    ),
    -- 6. Viagem, para fins de contagem de passageiros, com tolerância de 30 minutos,
    -- limitada pela viagem anterior
    viagem_com_tolerancia_previa as (
        select
            v.*,
            lag(v.datetime_chegada) over (
                partition by v.id_veiculo order by v.datetime_partida
            ) as viagem_anterior_chegada,
            case
                when
                    lag(v.datetime_chegada) over (
                        partition by v.id_veiculo order by v.datetime_partida
                    )
                    is null
                then
                    datetime(
                        timestamp_sub(
                            datetime_partida, interval {{ interval_minutes }} minute
                        )
                    )
                else
                    datetime(
                        timestamp_add(
                            greatest(
                                timestamp_sub(
                                    datetime_partida,
                                    interval {{ interval_minutes }} minute
                                ),
                                lag(v.datetime_chegada) over (
                                    partition by v.id_veiculo
                                    order by v.datetime_partida
                                )
                            ),
                            interval 1 second
                        )
                    )
            end as datetime_partida_com_tolerancia
        from viagem as v
    ),
    -- 7. Considera apenas as viagens realizadas no período de apuração
    viagem_com_tolerancia as (
        select *
        from viagem_com_tolerancia_previa
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
    ),
    -- 8. Contagem de transações Jaé
    transacao_contagem as (
        select
            v.data,
            v.id_viagem,
            count(t.datetime_transacao) as quantidade_transacao,
            countif(
                v.servico != t.servico_jae
            ) as quantidade_transacao_servico_divergente
        from transacao as t
        join
            viagem_com_tolerancia as v
            on t.id_veiculo = substr(v.id_veiculo, 2)
            and t.datetime_transacao
            between v.datetime_partida_com_tolerancia and v.datetime_chegada
        group by v.data, v.id_viagem
    ),
    -- 9. Contagem de transações RioCard
    transacao_riocard_contagem as (
        select
            v.data,
            v.id_viagem,
            count(tr.datetime_transacao) as quantidade_transacao_riocard,
            countif(
                v.servico != tr.servico_jae
            ) as quantidade_transacao_riocard_servico_divergente
        from transacao_riocard as tr
        join
            viagem_com_tolerancia as v
            on tr.id_veiculo = substr(v.id_veiculo, 2)
            and tr.datetime_transacao
            between v.datetime_partida_com_tolerancia and v.datetime_chegada
        group by v.data, v.id_viagem
    ),
    -- 10. Ajusta estado do equipamento
    -- Agrupa mesma posição para mesmo validador e veículo, mantendo preferencialmente
    -- o estado do equipamento "ABERTO" quanto latitude e longitude for diferente de
    -- (0,0)
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
                    from gps_validador
                    where
                        (
                            data >= date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                            and latitude != 0
                            and longitude != 0
                        )
                        or data < date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                    group by 1, 2, 3, 4, 5
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
                    from gps_validador
                    where
                        data >= date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                        and latitude = 0
                        and longitude = 0
                )
            )
    ),
    -- 11. Relacionamento entre estado do equipamento e viagem
    gps_validador_viagem as (
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
        from estado_equipamento_aux as e
        join
            viagem as v
            on e.id_veiculo = substr(v.id_veiculo, 2)
            and e.datetime_gps between v.datetime_partida and v.datetime_chegada
    ),
    -- 12. Calcula a porcentagem de estado do equipamento "ABERTO" por validador e
    -- viagem
    estado_equipamento_perc as (
        select
            data,
            id_viagem,
            id_validador,
            countif(servico != servico_jae) as quantidade_gps_servico_divergente,
            countif(estado_equipamento = "ABERTO")
            / count(*) as percentual_estado_equipamento_aberto
        from gps_validador_viagem
        group by 1, 2, 3
    ),
    -- 13. Calcula maior e menor porcentagem de estado do equipamento
    -- "ABERTO" por viagem
    estado_equipamento_max_min as (
        select
            data,
            id_viagem,
            sum(quantidade_gps_servico_divergente) as quantidade_gps_servico_divergente,
            max_by(
                id_validador, percentual_estado_equipamento_aberto
            ) as id_validador_max_perc,
            max(
                percentual_estado_equipamento_aberto
            ) as max_percentual_estado_equipamento_aberto,
            min(
                percentual_estado_equipamento_aberto
            ) as min_percentual_estado_equipamento_aberto,
        from estado_equipamento_perc
        group by 1, 2
    ),
    -- 14. Verifica se a viagem possui estado do equipamento "ABERTO" em pelo menos
    -- 80% dos registros
    estado_equipamento_verificacao as (
        select
            data,
            id_viagem,
            id_validador,
            quantidade_gps_servico_divergente,
            case
                when data < date('{{ var("DATA_SUBSIDIO_V15A_INICIO") }}')
                then max_percentual_estado_equipamento_aberto
                else min_percentual_estado_equipamento_aberto
            end as percentual_estado_equipamento_aberto,
            case
                when data < date('{{ var("DATA_SUBSIDIO_V15A_INICIO") }}')
                then
                    if(
                        max_percentual_estado_equipamento_aberto >= 0.8
                        or max_percentual_estado_equipamento_aberto is null,
                        true,
                        false
                    )
                else if(min_percentual_estado_equipamento_aberto >= 0.8, true, false)
            end as indicador_estado_equipamento_aberto
        from viagem
        left join estado_equipamento_max_min using (data, id_viagem)
    )
select
    v.data,
    v.id_viagem,
    v.id_veiculo,
    v.servico,
    eev.id_validador,
    case
        when
            v.data >= date("{{ var('DATA_SUBSIDIO_V8_INICIO') }}")
            and (
                (
                    v.data < date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                    and (
                        coalesce(tr.quantidade_transacao_riocard, 0) = 0
                        or coalesce(eev.indicador_estado_equipamento_aberto, false)
                        = false
                    )
                )
                or (
                    v.data >= date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                    and (
                        (
                            coalesce(tr.quantidade_transacao_riocard, 0) = 0
                            and coalesce(t.quantidade_transacao, 0) = 0
                        )
                        or coalesce(eev.indicador_estado_equipamento_aberto, false)
                        = false
                    )
                )
                or (
                    v.data >= date('{{ var("DATA_SUBSIDIO_V15A_INICIO") }}')
                    and (
                        (
                            coalesce(tr.quantidade_transacao_riocard, 0) = 0
                            and coalesce(t.quantidade_transacao, 0) = 0
                        )
                        or tr.quantidade_transacao_riocard_servico_divergente > 0
                        or t.quantidade_transacao_servico_divergente > 0
                        or eev.quantidade_gps_servico_divergente > 0
                        or coalesce(eev.indicador_estado_equipamento_aberto, false)
                        = false
                    )
                )
            )
            and ve.status
            in ("Licenciado com ar e não autuado", "Licenciado sem ar e não autuado")
            and v.datetime_partida not between "2024-10-06 06:00:00"
            and "2024-10-06 20:00:00"  -- Eleição (2024-10-06)
        then "Sem transação"
        else ve.status
    end as tipo_viagem,
    ve.tecnologia,
    v.sentido,
    v.distancia_planejada,
    coalesce(t.quantidade_transacao, 0) as quantidade_transacao,
    coalesce(tr.quantidade_transacao_riocard, 0) as quantidade_transacao_riocard,
    eev.percentual_estado_equipamento_aberto,
    eev.indicador_estado_equipamento_aberto,
    v.datetime_partida_com_tolerancia as datetime_partida_bilhetagem,
    v.datetime_partida,
    v.datetime_chegada,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_com_tolerancia as v
left join veiculos as ve using (data, id_veiculo)
left join transacao_contagem as t using (data, id_viagem)
left join transacao_riocard_contagem as tr using (data, id_viagem)
left join estado_equipamento_verificacao as eev using (data, id_viagem)
