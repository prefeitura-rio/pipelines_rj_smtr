{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        alias="viagem_transacao",
    )
}}

with
    -- 1. Transações Jaé
    transacao as (
        select id_veiculo, datetime_transacao
        from
            -- {{ ref("transacao") }}
            rj - smtr.br_rj_riodejaneiro_bilhetagem.transacao
        where
            data >= date("2024-10-01")
            {% if is_incremental() %}
                and data between date("{{ var(" start_date ") }}") and date_add(
                    date("{{ var(" end_date ") }}"), interval 1 day
                )
            {% endif %}
    ),
    -- 2. Transações RioCard
    transacao_riocard as (
        select id_veiculo, datetime_transacao
        from
            -- {{ ref("transacao_riocard") }}
            rj - smtr.br_rj_riodejaneiro_bilhetagem.transacao_riocard
        where
            data >= date("2024-10-01")
            {% if is_incremental() %}
                and data between date("{{ var(" start_date ") }}") and date_add(
                    date("{{ var(" end_date ") }}"), interval 1 day
                )
            {% endif %}
    ),
    -- 3. GPS Validador
    gps_validador as (
        select
            data,
            datetime_gps,
            id_veiculo,
            id_validador,
            estado_equipamento,
            latitude,
            longitude
        from
            -- {{ ref("gps_validador") }}
            rj - smtr.br_rj_riodejaneiro_bilhetagem.gps_validador
        where
            data >= date("2024-10-01")
            {% if is_incremental() %}
                and data between date("{{ var(" start_date ") }}") and date_add(
                    date("{{ var(" end_date ") }}"), interval 1 day
                )
            {% endif %}
            and (latitude != 0 or longitude != 0)
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
            sentido,
            distancia_planejada
        from
            -- {{ ref("viagem_completa") }}
            rj - smtr.projeto_subsidio_sppo.viagem_completa
        where
            data >= date("2024-10-01")
            {% if is_incremental() %}
                and data between date("{{ var(" start_date ") }}") and date(
                    "{{ var(" end_date ") }}"
                )
            {% endif %}
    ),
    -- 5. Status dos veículos 
    veiculos as (
        select data, id_veiculo, status
        from
            -- {{ ref("sppo_veiculo_dia") }}
            rj - smtr.veiculo.sppo_veiculo_dia
        where
            data >= date("2024-10-01")
            {% if is_incremental() %}
                and data between date("{{ var(" start_date ") }}") and date(
                    "{{ var(" end_date ") }}"
                )
            {% endif %}
    ),
    -- 6. Viagem, para fins de contagem de passageiros, com tolerância de 30 minutos,
    -- limitada pela viagem anterior
    viagem_com_tolerancia as (
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
                then datetime(timestamp_sub(datetime_partida, interval 2 hour))
                else
                    datetime(
                        timestamp_add(
                            greatest(
                                timestamp_sub(datetime_partida, interval 2 hour),
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
    -- 7. Contagem de transações Jaé
    transacao_contagem as (
        select v.data, v.id_viagem, count(t.datetime_transacao) as quantidade_transacao
        from transacao as t
        join
            viagem_com_tolerancia as v
            on t.id_veiculo = substr(v.id_veiculo, 2)
            and t.datetime_transacao
            between v.datetime_partida_com_tolerancia and v.datetime_chegada
        group by v.data, v.id_viagem
    ),
    -- 5. Contagem de transações RioCard
    transacao_riocard_contagem as (
        select
            v.data,
            v.id_viagem,
            count(tr.datetime_transacao) as quantidade_transacao_riocard
        from transacao_riocard as tr
        join
            viagem_com_tolerancia as v
            on tr.id_veiculo = substr(v.id_veiculo, 2)
            and tr.datetime_transacao
            between v.datetime_partida_com_tolerancia and v.datetime_chegada
        group by v.data, v.id_viagem
    ),
    -- 6. Ajusta estado do equipamento
    -- Agrupa mesma posição para mesmo validador e veículo, mantendo preferencialmente
    -- o estado do equipamento "ABERTO"
    estado_equipamento_aux as (
        select
            data,
            id_validador,
            id_veiculo,
            latitude,
            longitude,
            if(
                count(case when estado_equipamento = "ABERTO" then 1 end) >= 1,
                "ABERTO",
                "FECHADO"
            ) as estado_equipamento,
            min(datetime_gps) as datetime_gps,
        from gps_validador
        group by 1, 2, 3, 4, 5
    ),
    -- 7. Relacionamento entre estado do equipamento e viagem
    gps_validador_viagem as (
        select
            v.data,
            e.datetime_gps,
            v.id_viagem,
            e.id_validador,
            e.estado_equipamento,
            e.latitude,
            e.longitude
        from estado_equipamento_aux as e
        join
            viagem as v
            on e.id_veiculo = substr(v.id_veiculo, 2)
            and e.datetime_gps between v.datetime_partida and v.datetime_chegada
    ),
    -- 8. Calcula a porcentagem de estado do equipamento "ABERTO" por validador e viagem
    estado_equipamento_perc as (
        select
            data,
            id_viagem,
            id_validador,
            countif(estado_equipamento = "ABERTO")
            / count(*) as percentual_estado_equipamento_aberto
        from gps_validador_viagem
        group by 1, 2, 3
    ),
    -- 9. Considera o validador com maior porcentagem de estado do equipamento
    -- "ABERTO" por viagem
    estado_equipamento_max_perc as (
        select
            data,
            id_viagem,
            max_by(id_validador, percentual_estado_equipamento_aberto) as id_validador,
            max(
                percentual_estado_equipamento_aberto
            ) as percentual_estado_equipamento_aberto
        from estado_equipamento_perc
        group by 1, 2
    ),
    -- 10. Verifica se a viagem possui estado do equipamento "ABERTO" em pelo menos
    -- 80% dos registros
    estado_equipamento_verificacao as (
        select
            data,
            id_viagem,
            id_validador,
            percentual_estado_equipamento_aberto,
            if(
                percentual_estado_equipamento_aberto >= 0.8
                or percentual_estado_equipamento_aberto is null,
                true,
                false
            ) as indicador_estado_equipamento_aberto
        from viagem
        left join estado_equipamento_max_perc using (data, id_viagem)
    )
select
    v.data,
    v.id_viagem,
    v.id_veiculo,
    v.servico,
    case
        when
            v.data >= date("2024-10-01")
            and (
                coalesce(tr.quantidade_transacao_riocard, 0) = 0
                or coalesce(eev.indicador_estado_equipamento_aberto, false) = false
            )
            and ve.status
            in ("Licenciado com ar e não autuado", "Licenciado sem ar e não autuado")
        then "Sem transação"
        else ve.status
    end as tipo_viagem,
    v.sentido,
    v.distancia_planejada,
    coalesce(t.quantidade_transacao, 0) as quantidade_transacao,
    coalesce(tr.quantidade_transacao_riocard, 0) as quantidade_transacao_riocard,
    v.datetime_partida_com_tolerancia as datetime_partida_bilhetagem,
    v.datetime_partida,
    v.datetime_chegada,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_com_tolerancia as v
left join veiculos as ve using (data, id_veiculo)
left join transacao_contagem as t using (data, id_viagem)
left join transacao_riocard_contagem as tr using (data, id_viagem)
left join estado_equipamento_verificacao as eev using (data, id_viagem)