{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_viagem"],
        incremental_strategy="insert_overwrite",
    )
}}

{%- if execute %}
    {% set query = (
        "SELECT DISTINCT COALESCE(feed_start_date, data_versao_trips, data_versao_shapes, data_versao_frequencies) FROM "
        ~ ref("subsidio_data_versao_efetiva")
        ~ " WHERE data BETWEEN DATE('"
        ~ var("start_date")
        ~ "') AND DATE('"
        ~ var("end_date")
        ~ "')"
    ) %}
    {{- log(query, info=True) -}}
    {% set feed_start_dates = run_query(query).columns[0].values() %}
    {{- log(feed_start_dates, info=True) -}}
{% endif -%}

with
    -- 1. Viagens planejadas (agrupadas por data e serviço)
    planejado as (
        select distinct
            data,
            tipo_dia,
            consorcio,
            servico,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            partidas_total_planejada,
            distancia_total_planejada as km_planejada,
            if(sentido = "C", true, false) as indicador_circular
        from {{ ref("viagem_planejada") }}
        -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and (distancia_total_planejada > 0 or distancia_total_planejada is null)
            and (id_tipo_trajeto = 0 or id_tipo_trajeto is null)
    ),
    viagens_planejadas as (
        select
            feed_start_date,
            servico,
            tipo_dia,
            viagens_planejadas,
            partidas_ida,
            partidas_volta,
            tipo_os,
        from {{ ref("ordem_servico_gtfs") }}
        -- `rj-smtr.gtfs.ordem_servico`
        where feed_start_date in ('{{ feed_start_dates|join("', '") }}')
    ),
    data_versao_efetiva as (
        select
            data,
            tipo_dia,
            tipo_os,
            coalesce(
                feed_start_date,
                data_versao_trips,
                data_versao_shapes,
                data_versao_frequencies
            ) as feed_start_date
        from {{ ref("subsidio_data_versao_efetiva") }}
        -- `rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva`
        -- (alterar também query no bloco execute)
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    viagem_planejada as (
        select
            p.data,
            p.tipo_dia,
            p.consorcio,
            p.servico,
            p.faixa_horaria_inicio,
            p.faixa_horaria_fim,
            v.viagens_planejadas,
            p.km_planejada,
            if(
                p.data >= date('{{ var("DATA_SUBSIDIO_V9_INICIO") }}'),
                p.partidas_total_planejada,
                v.partidas_ida + v.partidas_volta
            ) as viagens_planejadas_ida_volta,
            p.indicador_circular
        from planejado as p
        left join data_versao_efetiva as d using (data, tipo_dia)
        left join
            viagens_planejadas as v
            on d.feed_start_date = v.feed_start_date
            and p.tipo_dia = v.tipo_dia
            and p.servico = v.servico
            and (d.tipo_os = v.tipo_os or (d.tipo_os is null and v.tipo_os = "Regular"))
    ),
    -- 2. Parâmetros de subsídio
    subsidio_parametros as (
        select distinct
            data_inicio,
            data_fim,
            status,
            tecnologia,
            subsidio_km,
            max(subsidio_km) over (
                partition by date_trunc(data_inicio, year), data_fim
            ) as subsidio_km_teto,
            indicador_penalidade_judicial
        from {{ ref("valor_km_tipo_viagem") }}
    -- `rj-smtr.subsidio.valor_km_tipo_viagem`
    ),
    tecnologias as (
        select
            servico,
            case
                when substring(codigo_tecnologia, 4, 1) = "1"
                then "PADRON"
                when substring(codigo_tecnologia, 3, 1) = "1"
                then "BASICO"
                when substring(codigo_tecnologia, 2, 1) = "1"
                then "MIDI"
                when substring(codigo_tecnologia, 1, 1) = "1"
                then "MINI"
                else null
            end as maior_tecnologia_permitida,
            case
                when substring(codigo_tecnologia, 1, 1) = "1"
                then "MINI"
                when substring(codigo_tecnologia, 2, 1) = "1"
                then "MIDI"
                when substring(codigo_tecnologia, 3, 1) = "1"
                then "BASICO"
                when substring(codigo_tecnologia, 4, 1) = "1"
                then "PADRON"
                else null
            end as menor_tecnologia_permitida,
        from {{ var("tecnologia_servico") }}
    ),
    prioridade_tecnologia as (
        select "MINI" as tecnologia, 1 as prioridade
        union all
        select "MIDI" as tecnologia, 2 as prioridade
        union all
        select "BASICO" as tecnologia, 3 as prioridade
        union all
        select "PADRON" as tecnologia, 4 as prioridade
    ),
    -- 3. Viagens com quantidades de transações
    viagem_transacao as (
        select *
        from {{ ref("viagem_transacao") }}
        -- `rj-smtr.subsidio.viagem_transacao`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    -- 4. Viagens com tipo e valor de subsídio por km
    viagem_tecnologia as (
        select
            vt.data,
            vt.servico,
            vt.tipo_viagem,
            vt.tecnologia as tecnologia_apurada,
            case
                when p.prioridade > p_maior.prioridade
                then t.maior_tecnologia_permitida
                when
                    p.prioridade < p_menor.prioridade
                    and data >= date('{{ var("DATA_SUBSIDIO_V14A_INICIO") }}')
                then null
                else vt.tecnologia
            end as tecnologia_remunerada,
            vt.id_viagem,
            vt.datetime_partida,
            vt.distancia_planejada,
            case
                when p.prioridade < p_menor.prioridade then true else false
            end as indicador_penalidade_tecnologia
        from viagem_transacao as vt
        left join tecnologias as t on vt.servico = t.servico
        left join prioridade_tecnologia as p on vt.tecnologia = p.tecnologia
        left join
            prioridade_tecnologia as p_maior
            on t.maior_tecnologia_permitida = p_maior.tecnologia
        left join
            prioridade_tecnologia as p_menor
            on t.menor_tecnologia_permitida = p_menor.tecnologia
    ),
    viagem_km_tipo as (
        select
            vt.data,
            vt.servico,
            vt.tipo_viagem,
            vt.tecnologia_apurada,
            vt.tecnologia_remunerada,
            vt.id_viagem,
            vt.datetime_partida,
            vt.distancia_planejada,
            sp.subsidio_km,
            sp.subsidio_km_teto,
            vt.indicador_penalidade_tecnologia,
            sp.indicador_penalidade_judicial
        from viagem_tecnologia as vt
        left join
            subsidio_parametros as sp
            on vt.data between sp.data_inicio and sp.data_fim
            and vt.tipo_viagem = sp.status
            and (
                vt.tecnologia_remunerada = sp.tecnologia
                or (vt.tecnologia_remunerada is null and sp.tecnologia is null)
            )
    ),
    -- 5. Apuração de km realizado e Percentual de Operação Diário (POD)
    servico_faixa_km_apuracao as (
        select
            p.data,
            p.tipo_dia,
            p.faixa_horaria_inicio,
            p.faixa_horaria_fim,
            p.consorcio,
            p.servico,
            p.km_planejada as km_planejada,
            coalesce(
                round(
                    100 * sum(
                        if(
                            v.tipo_viagem not in ("Não licenciado", "Não vistoriado"),
                            v.distancia_planejada,
                            0
                        )
                    )
                    / p.km_planejada,
                    2
                ),
                0
            ) as pof
        from viagem_planejada as p
        left join
            viagem_km_tipo as v
            on p.data = v.data
            and p.servico = v.servico
            and v.datetime_partida
            between p.faixa_horaria_inicio and p.faixa_horaria_fim
        group by 1, 2, 3, 4, 5, 6, 7
    )
-- 6. Flag de viagens que serão consideradas ou não para fins de remuneração (apuração
-- de valor de subsídio) - RESOLUÇÃO SMTR Nº 3645/2023
select
    v.* except (
        rn,
        datetime_partida,
        viagens_planejadas,
        viagens_planejadas_ida_volta,
        km_planejada,
        tipo_dia,
        consorcio,
        faixa_horaria_inicio,
        faixa_horaria_fim,
        indicador_circular
    ),
    case
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}')
            and v.tipo_dia = "Dia Útil"
            and viagens_planejadas < 10
            and viagens_planejadas > 5
            and pof > 100
            and rn > (viagens_planejadas_ida_volta + if(indicador_circular, 1, 2))
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}')
            and v.tipo_dia = "Dia Útil"
            and viagens_planejadas >= 10
            and pof > 110
            and rn > viagens_planejadas_ida_volta * 1.1
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}')
            and v.tipo_dia = "Dia Útil"
            and viagens_planejadas <= 5
            and pof > 200
            and rn > viagens_planejadas_ida_volta * 2
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}')
            and v.tipo_dia != "Dia Útil"
            and viagens_planejadas < 5
            and pof > 100
            and rn > (viagens_planejadas_ida_volta + if(indicador_circular, 1, 2))
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}')
            and v.tipo_dia != "Dia Útil"
            and viagens_planejadas >= 5
            and pof > 120
            and rn > viagens_planejadas_ida_volta * 1.2
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}')
            and v.tipo_dia = "Dia Útil"
            and viagens_planejadas > 10
            and pof > 120
            and rn > viagens_planejadas_ida_volta * 1.2
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}')
            and v.tipo_dia = "Dia Útil"
            and viagens_planejadas <= 10
            and pof > 200
            and rn > viagens_planejadas_ida_volta * 2
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}')
            and (
                v.tipo_dia = "Dia Útil"
                and (viagens_planejadas is null or pof is null or rn is null)
            )
        then null
        else true
    end as indicador_viagem_dentro_limite,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from
    (
        select
            v.*,
            p.* except (data, servico),
            row_number() over (
                partition by v.data, v.servico, faixa_horaria_inicio, faixa_horaria_fim
                order by subsidio_km * distancia_planejada desc
            ) as rn
        from viagem_km_tipo as v
        left join
            viagem_planejada as p
            on p.data = v.data
            and p.servico = v.servico
            and v.datetime_partida
            between p.faixa_horaria_inicio and p.faixa_horaria_fim
    ) as v
left join
    servico_faixa_km_apuracao as s
    on s.data = v.data
    and s.servico = v.servico
    and v.datetime_partida between s.faixa_horaria_inicio and s.faixa_horaria_fim
