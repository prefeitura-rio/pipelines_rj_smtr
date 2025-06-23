{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    -- 1. Viagens planejadas
    planejado as (
        select distinct
            data,
            tipo_dia,
            consorcio,
            servico,
            "Ônibus SPPO" as modo,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            distancia_total_planejada as km_planejada
        {# from {{ ref("viagem_planejada") }} #}
        from `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
            and distancia_total_planejada > 0
        union all
        select distinct
            data,
            tipo_dia,
            consorcio,
            servico,
            modo,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            quilometragem as km_planejada
        from {{ ref("servico_planejado_faixa_horaria") }}
        -- from `rj-smtr.planejamento.servico_planejado_faixa_horaria`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
            and quilometragem > 0
    ),
    -- 2. Viagens realizadas
    viagem as (
        select
            data, servico, id_viagem, tipo_viagem, datetime_partida, distancia_planejada
        from {{ ref("viagem_transacao") }}
        -- from `rj-smtr.subsidio.viagem_transacao`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
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
                                v.tipo_viagem
                                not in ("Não licenciado", "Não vistoriado"),
                                v.distancia_planejada,
                                0
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
    viagens_faixa,
    km_apurada_faixa,
    km_planejada_faixa,
    pof,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from servico_km_apuracao
