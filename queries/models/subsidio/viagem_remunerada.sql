{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

with
    viagens as (
        select *
        from {{ ref("viagem_transacao") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    servico_planejado as (
        select *
        from {{ ref("servico_planejado") }}
        where
            viagens_dia > 0
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    ),
    subsidio_parametros as (
        select distinct
            data_inicio,
            data_fim,
            status,
            tecnologia,
            modo,
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
            indicador_penalidade_judicial
        from {{ ref("valor_km_tipo_viagem") }}
    -- from `rj-smtr.subsidio.valor_km_tipo_viagem`
    ),
    viagem_tecnologia as (
        select distinct
            v.data,
            v.servico,
            v.tipo_viagem,
            v.tecnologia as tecnologia_apurada,
            case
                when p.prioridade > p_maior.prioridade
                then t.maior_tecnologia_permitida
                when
                    p.prioridade < p_menor.prioridade
                    and data >= date('{{ var("DATA_SUBSIDIO_V14A_INICIO") }}')
                then null
                else v.tecnologia
            end as tecnologia_remunerada,
            v.id_viagem,
            v.modo,
            v.datetime_partida,
            v.distancia_planejada,
            {# v.feed_start_date #}
            case
                when p.prioridade < p_menor.prioridade then true else false
            end as indicador_penalidade_tecnologia
        from viagens as v
        left join {{ ref("tecnologia_servico") }} as t on v.servico = t.servico
        {# and v.feed_start_date = t.feed_start_date #}
        left join
            {{ ref("tecnologia_prioridade") }} as p
            on v.tecnologia = p.tecnologia
            and v.modo = p.modo
        left join
            {{ ref("tecnologia_prioridade") }} as p_maior
            on t.maior_tecnologia_permitida = p_maior.tecnologia
            and v.modo = p_maior.modo
        left join
            {{ ref("tecnologia_prioridade") }} as p_menor
            on t.menor_tecnologia_permitida = p_menor.tecnologia
            and v.modo = p_menor.modo
    ),
    servico_faixa_km_apuracao as (
        select
            s.data,
            s.tipo_dia,
            s.faixa_horaria_inicio,
            s.faixa_horaria_fim,
            s.consorcio,
            s.servico,
            s.quilometragem as km_planejada,
            coalesce(
                round(
                    100 * sum(
                        if(
                            v.tipo_viagem not in ("Não licenciado", "Não vistoriado"),
                            v.distancia_planejada,
                            0
                        )
                    )
                    / s.quilometragem,
                    2
                ),
                0
            ) as pof
        from servico_planejado as s
        left join
            viagem_tecnologia as v
            on s.data = v.data
            and s.servico = v.servico
            and v.datetime_partida
            between s.faixa_horaria_inicio and s.faixa_horaria_fim
        group by 1, 2, 3, 4, 5, 6, 7
    ),
    viagem_km_tipo as (
        select distinct
            vt.data,
            vt.servico,
            vt.tipo_viagem,
            vt.tecnologia_apurada,
            vt.tecnologia_remunerada,
            vt.id_viagem,
            vt.modo,
            vt.datetime_partida,
            vt.distancia_planejada,
            sp.subsidio_km,
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
            vt.indicador_penalidade_tecnologia,
            sp.indicador_penalidade_judicial
        from viagem_tecnologia as vt
        left join
            subsidio_parametros as sp
            on vt.data between sp.data_inicio and sp.data_fim
            and vt.tipo_viagem = sp.status
            and (
                (
                    vt.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                    and (
                        vt.tecnologia_remunerada = sp.tecnologia
                        or (vt.tecnologia_remunerada is null and sp.tecnologia is null)
                    )
                )
                or (
                    vt.data < date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                    and sp.tecnologia is null
                )
            )
        left join
            subsidio_parametros as ta
            on vt.data between ta.data_inicio and ta.data_fim
            and vt.tipo_viagem = ta.status
            and (
                vt.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                and (
                    vt.tecnologia_apurada = ta.tecnologia
                    or (vt.tecnologia_apurada is null and ta.tecnologia is null)
                )
            )
        left join
            servico_faixa_km_apuracao as s
            on s.data = vt.data
            and s.servico = vt.servico
            and vt.datetime_partida
            between s.faixa_horaria_inicio and s.faixa_horaria_fim

    )
select
    v.* except (
        rn,
        datetime_partida,
        viagens_dia,
        partidas,
        km_planejada,
        tipo_dia,
        consorcio,
        faixa_horaria_inicio,
        faixa_horaria_fim,
        extensao_ida,
        extensao_volta,
    ),
    case
        when
            v.tipo_dia = "Dia Útil"
            and viagens_dia < 10
            and viagens_dia > 5
            and pof > 100
            and rn > (partidas + if(sentido = "C", 1, 2))
        then false
        when
            v.tipo_dia = "Dia Útil"
            and viagens_dia >= 10
            and pof > 110
            and rn > partidas * 1.1
        then false
        when
            v.tipo_dia = "Dia Útil"
            and viagens_dia <= 5
            and pof > 200
            and rn > partidas * 2
        then false
        when
            v.tipo_dia != "Dia Útil"
            and viagens_dia < 5
            and pof > 100
            and rn > (partidas + if(sentido = "C", 1, 2))
        then false
        when
            v.tipo_dia != "Dia Útil"
            and viagens_dia >= 5
            and pof > 120
            and rn > partidas * 1.2
        then false
        else true
    end as indicador_viagem_dentro_limite,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from
    (
        select
            v.*,
            sp.* except (data, servico),
            row_number() over (
                partition by v.data, v.servico, faixa_horaria_inicio, faixa_horaria_fim
                order by subsidio_km * distancia_planejada desc
            ) as rn
        from viagem_km_tipo as v
        left join
            servico_planejado as sp
            on sp.data = v.data
            and sp.servico = v.servico
            and v.datetime_partida
            between sp.faixa_horaria_inicio and sp.faixa_horaria_fim
    ) as v
left join
    servico_faixa_km_apuracao as s
    on s.data = v.data
    and s.servico = v.servico
    and v.datetime_partida between s.faixa_horaria_inicio and s.faixa_horaria_fim
