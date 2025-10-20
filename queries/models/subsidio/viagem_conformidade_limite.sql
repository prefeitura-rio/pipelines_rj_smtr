{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
{% endset %}

with
    viagens as (
        select * from {{ ref("viagem_transacao") }} where {{ incremental_filter }}
    ),
    planejado as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            partidas as viagens_planejadas,
            quilometragem as km_planejada
        from {{ ref("servico_planejado_faixa_horaria") }}
        where quilometragem > 0 and {{ incremental_filter }}
    ),
    subsidio_parametros as (
        select distinct
            data_inicio,
            data_fim,
            status,
            tecnologia,
            "Ônibus SPPO" as modo,
            subsidio_km,
            case
                when tecnologia is null
                then
                    max(subsidio_km) over (
                        partition by date_trunc(data_inicio, year), data_fim
                    )
                when tecnologia is not null
                then
                    max(subsidio_km) over (
                        partition by date_trunc(data_inicio, year), data_fim, tecnologia
                    )
            end as subsidio_km_teto,
            indicador_penalidade_judicial,
            ordem
        from {{ ref("valor_km_tipo_viagem") }}
    ),
    servico_faixa_km_apuracao as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            sentido,
            km_planejada_faixa as km_planejada,
            pof
        from {{ ref("percentual_operacao_faixa_horaria") }}
        where {{ incremental_filter }}
    ),
    viagem_km_tipo as (
        select distinct
            vt.data,
            vt.servico,
            vt.sentido,
            vt.modo,
            vt.tipo_viagem,
            vt.tecnologia_apurada,
            vt.tecnologia_remunerada,
            vt.id_viagem,
            vt.id_veiculo,
            vt.datetime_partida,
            vt.distancia_planejada,
            s.pof,
            case
                when vt.tipo_viagem = "Não autorizado por capacidade"
                then 0
                else sp.subsidio_km
            end as subsidio_km,
            sp.subsidio_km_teto,
            case
                when
                    vt.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                    and s.pof >= 80
                    and vt.tipo_viagem in (
                        "Licenciado com ar e não autuado",
                        "Licenciado sem ar e não autuado"
                    )
                then
                    safe_cast(
                        - (
                            ta.subsidio_km * vt.distancia_planejada
                            - sp.subsidio_km * vt.distancia_planejada
                        ) as numeric
                    )
                else safe_cast(0 as numeric)
            end as valor_glosado_tecnologia,
            if(
                vt.tipo_viagem = "Não autorizado por capacidade", true, false
            ) as indicador_penalidade_tecnologia,
            sp.indicador_penalidade_judicial,
            sp.ordem
        from viagens as vt
        left join
            subsidio_parametros as sp
            on vt.data between sp.data_inicio and sp.data_fim
            and vt.tipo_viagem = sp.status
            and vt.data >= date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
        left join
            subsidio_parametros as ta
            on vt.data between ta.data_inicio and ta.data_fim
            and vt.tipo_viagem = ta.status
            and (
                vt.data
                between date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}') and date_sub(
                    date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}'), interval 1 day
                )
                and (
                    vt.tecnologia_apurada = ta.tecnologia
                    or (vt.tecnologia_apurada is null and ta.tecnologia is null)
                )
            )
        left join
            servico_faixa_km_apuracao as s
            on s.data = vt.data
            and s.servico = vt.servico
            and s.sentido = vt.sentido
            and vt.datetime_partida
            between s.faixa_horaria_inicio and s.faixa_horaria_fim
    )
select
    v.* except (rn, viagens_planejadas, km_planejada, ordem),
    case
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V17_INICIO") }}')
            and v.tipo_dia in ("Sabado", "Domingo")
            and v.pof > 120
            and rn > greatest((viagens_planejadas * 1.2), (viagens_planejadas + 1))
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V17_INICIO") }}')
            and v.tipo_dia = "Ponto Facultativo"
            and v.pof > 150
            and rn > greatest((viagens_planejadas * 1.5), (viagens_planejadas + 1))
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V17_INICIO") }}')
            and v.tipo_dia = "Dia Útil"
            and v.pof > 110
            and rn > greatest((viagens_planejadas * 1.1), (viagens_planejadas + 1))
        then false
        else true
    end as indicador_viagem_dentro_limite,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from
    (
        select
            v.*,
            p.* except (data, servico, sentido),
            row_number() over (
                partition by
                    v.data,
                    v.servico,
                    v.sentido,
                    faixa_horaria_inicio,
                    faixa_horaria_fim
                order by ordem, datetime_partida
            ) as rn
        from viagem_km_tipo as v
        left join
            planejado as p
            on p.data = v.data
            and p.servico = v.servico
            and p.sentido = v.sentido
            and v.datetime_partida
            between p.faixa_horaria_inicio and p.faixa_horaria_fim
    ) as v
left join
    servico_faixa_km_apuracao as s
    on s.data = v.data
    and s.servico = v.servico
    and s.sentido = v.sentido
    and v.datetime_partida between s.faixa_horaria_inicio and s.faixa_horaria_fim
