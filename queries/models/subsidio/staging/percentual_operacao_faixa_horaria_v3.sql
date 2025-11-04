{{ config(materialized="ephemeral") }}
{% set incremental_filter %}
    data between greatest(date("{{var('start_date')}}"), date("{{ var('DATA_SUBSIDIO_V22_INICIO') }}")) and date("{{ var('end_date') }}")
{% endset %}
with
    -- 1. Viagens planejadas
    planejado as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            sentido,
            modo,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            quilometragem as km_planejada
        {# from {{ ref("servico_planejado_faixa_horaria") }} #}
        from `rj-smtr-dev`.`botelho__planejamento`.`servico_planejado_faixa_horaria`
        where quilometragem > 0 and {{ incremental_filter }}
    ),
    -- 2. Viagens realizadas
    viagem as (
        select
            data,
            servico,
            sentido,
            modo,
            id_viagem,
            tipo_viagem,
            datetime_partida,
            distancia_planejada
        from {{ ref("viagem_transacao") }}
        -- from `rj-smtr.subsidio.viagem_transacao`
        where {{ incremental_filter }}
    ),
    -- 3. Apuração de km realizado e Percentual de Operação por faixa
    servico_km_apuracao as (
        select
            p.data,
            p.tipo_dia,
            p.faixa_horaria_inicio,
            p.faixa_horaria_fim,
            p.consorcio,
            p.servico,
            p.modo,
            p.sentido,
            safe_cast(p.km_planejada as numeric) as km_planejada_faixa,
            safe_cast(coalesce(count(v.id_viagem), 0) as int64) as viagens_faixa,
            safe_cast(
                coalesce(sum(v.distancia_planejada), 0) as numeric
            ) as km_apurada_faixa,
            safe_cast(
                coalesce(
                    round(
                        100 * sum(
                            if(
                                (
                                    p.data
                                    < date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
                                    and v.tipo_viagem
                                    in ('Não licenciado', 'Não vistoriado')
                                )
                                or (
                                    p.data
                                    >= date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
                                    and v.tipo_viagem in (
                                        'Não licenciado',
                                        'Não vistoriado',
                                        'Lacrado',
                                        'Não autorizado por ausência de ar-condicionado'
                                    )
                                ),
                                0,
                                v.distancia_planejada
                            )
                        )
                        / p.km_planejada,
                        2
                    ),
                    0
                ) as numeric
            ) as pof
        from planejado as p
        left join
            viagem as v
            on p.data = v.data
            and p.servico = v.servico
            and p.sentido = v.sentido
            and p.modo = v.modo
            and v.datetime_partida
            between p.faixa_horaria_inicio and p.faixa_horaria_fim
        group by
            p.data,
            p.tipo_dia,
            p.faixa_horaria_inicio,
            p.faixa_horaria_fim,
            p.consorcio,
            p.servico,
            p.modo,
            p.sentido,
            p.km_planejada
    )
select
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    modo,
    sentido,
    viagens_faixa,
    km_apurada_faixa,
    km_planejada_faixa,
    pof,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from servico_km_apuracao
