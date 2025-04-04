{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between
        date('{{ var("start_date") }}')
        and date('{{ var("end_date") }}')
{% endset %}

with
    viagens as (
        select *
        from {{ ref("viagem_transacao") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    servico_planejado_faixa_horaria as (
        select
            data,
            servico,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            sum(partidas) over (
                partition by data, servico, faixa_horaria_inicio
            ) as partidas,
            sum(quilometragem) over (
                partition by data, servico, faixa_horaria_inicio
            ) as km_planejada
        from {{ ref("servico_planejado_faixa_horaria") }}
        where
            quilometragem > 0
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    ),
    servico_planejado_faixa_horaria as (
        select data, servico, viagens
        from {{ ref("servico_planejado_dia") }}
        where {{ incremental_filter }}
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
            indicador_penalidade_judicial
        from {{ ref("valor_km_tipo_viagem") }}
    {# from `rj-smtr.subsidio.valor_km_tipo_viagem` #}
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
            v.sentido,
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
            s.faixa_horaria_inicio,
            s.faixa_horaria_fim,
            s.servico,
            coalesce(
                round(
                    100 * sum(
                        if(
                            v.tipo_viagem not in ("Não licenciado", "Não vistoriado"),
                            v.distancia_planejada,
                            0
                        )
                    )
                    / s.km_planejada,
                    2
                ),
                0
            ) as pof
        from servico_planejado_faixa_horaria as s
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
            vt.id_viagem,
            vt.distancia_planejada,
            vt.tecnologia_apurada,
            vt.tecnologia_remunerada,
            vt.sentido,
            vt.modo,
            vt.datetime_partida,
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
        sentido,
        viagens,
        partidas,
        km_planejada,
        faixa_horaria_inicio,
        faixa_horaria_fim,
    ),
    case
        when
            v.tipo_dia = "Dia Útil"
            and viagens < 10
            and viagens > 5
            and pof > 100
            and rn > (partidas + if(sentido = "C", 1, 2))
        then false
        when
            v.tipo_dia = "Dia Útil"
            and viagens >= 10
            and pof > 110
            and rn > partidas * 1.1
        then false
        when
            v.tipo_dia = "Dia Útil" and viagens <= 5 and pof > 200 and rn > partidas * 2
        then false
        when
            v.tipo_dia != "Dia Útil"
            and viagens < 5
            and pof > 100
            and rn > (partidas + if(sentido = "C", 1, 2))
        then false
        when
            v.tipo_dia != "Dia Útil"
            and viagens >= 5
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
            spf.faixa_horaria_inicio,
            spf.faixa_horaria_fim,
            spf.partidas,
            spf.km_planejada,
            spd.viagens,
            row_number() over (
                partition by
                    v.data, v.servico, spf.faixa_horaria_inicio, spf.faixa_horaria_fim
                order by subsidio_km * distancia_planejada desc
            ) as rn
        from viagem_km_tipo as v
        left join
            servico_planejado_faixa_horaria as spf
            on spf.data = v.data
            and spf.servico = v.servico
            and v.datetime_partida
            between spf.faixa_horaria_inicio and spf.faixa_horaria_fim
        left join
            servico_planejado_dia as spd
            on spd.data = v.data
            and spd.servico = v.servico
    ) as v
left join
    servico_faixa_km_apuracao as s
    on s.data = v.data
    and s.servico = v.servico
    and v.datetime_partida between s.faixa_horaria_inicio and s.faixa_horaria_fim
