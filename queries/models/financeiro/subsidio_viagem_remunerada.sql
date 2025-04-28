{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    subsidio_faixa_dia as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            modo,
            pof
        from {{ ref("subsidio_faixa_servico_dia") }}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    servico_km_apuracao as (
        select
            data,
            servico,
            case
                when tipo_viagem = "Nao licenciado"
                then "Não licenciado"
                when tipo_viagem = "Licenciado com ar e autuado (023.II)"
                then "Autuado por ar inoperante"
                when tipo_viagem = "Licenciado sem ar"
                then "Licenciado sem ar e não autuado"
                when tipo_viagem = "Licenciado com ar e não autuado (023.II)"
                then "Licenciado com ar e não autuado"
                else tipo_viagem
            end as tipo_viagem,
            tecnologia_apurada,
            tecnologia_remunerada,
            id_viagem,
            datetime_partida,
            distancia_planejada,
            subsidio_km,
            subsidio_km_teto,
            valor_glosado_tecnologia,
            indicador_penalidade_judicial,
            indicador_viagem_dentro_limite
        from {{ ref("viagens_remuneradas") }}
        -- `rj-smtr.dashboard_subsidio_sppo.viagens_remuneradas`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
        union all
        select
            data,
            servico,
            tipo_viagem,
            tecnologia_apurada,
            tecnologia_remunerada,
            id_viagem,
            datetime_partida,
            distancia_planejada,
            subsidio_km,
            subsidio_km_teto,
            valor_glosado_tecnologia,
            indicador_penalidade_judicial,
            indicador_viagem_dentro_limite
        from {{ ref("viagem_dentro_limite") }}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
    ),
    indicador_ar as (
        select
            data,
            id_veiculo,
            "Ônibus SPPO" as modo,
            status,
            safe_cast(
                json_value(indicadores, "$.indicador_ar_condicionado") as bool
            ) as indicador_ar_condicionado
        from {{ ref("sppo_veiculo_dia") }}
        -- from `rj-smtr.veiculo.sppo_veiculo_dia`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
        union all
        select
            data,
            id_veiculo,
            modo,
            status,
            safe_cast(
                json_value(indicadores, "$.indicador_ar_condicionado") as bool
            ) as indicador_ar_condicionado
        from {{ ref("status_dia") }}
        -- from `rj-smtr.veiculo.status_dia`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
    ),
    viagem as (
        select
            data, servico_realizado as servico, id_veiculo, id_viagem, datetime_partida
        from {{ ref("viagem_completa") }}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
        union all
        select data, servico, id_veiculo, id_viagem, datetime_partida
        from {{ ref("viagem_valida") }}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
    ),
    ar_viagem as (
        select
            v.data,
            v.servico,
            v.id_viagem,
            v.id_veiculo,
            v.datetime_partida,
            coalesce(ia.indicador_ar_condicionado, false) as indicador_ar_condicionado
        from viagem v
        left join indicador_ar ia on ia.data = v.data and ia.id_veiculo = v.id_veiculo
    ),
    subsidio_servico_ar as (
        select
            sfd.data,
            sfd.tipo_dia,
            sfd.faixa_horaria_inicio,
            sfd.faixa_horaria_fim,
            sfd.consorcio,
            sfd.servico,
            sfd.pof,
            av.id_veiculo,
            coalesce(s.tipo_viagem, "Sem viagem apurada") as tipo_viagem,
            s.tecnologia_apurada,
            s.tecnologia_remunerada,
            s.id_viagem,
            av.datetime_partida,
            safe_cast(s.distancia_planejada as numeric) as distancia_planejada,
            safe_cast(s.subsidio_km as numeric) as subsidio_km,
            safe_cast(s.subsidio_km_teto as numeric) as subsidio_km_teto,
            coalesce(s.valor_glosado_tecnologia, 0) as valor_glosado_tecnologia,
            s.indicador_viagem_dentro_limite,
            case
                when sfd.pof < 60 then true else s.indicador_penalidade_judicial
            end as indicador_penalidade_judicial,
            coalesce(av.indicador_ar_condicionado, false) as indicador_ar_condicionado
        from subsidio_faixa_dia as sfd
        left join
            ar_viagem as av
            on sfd.data = av.data
            and sfd.servico = av.servico
            and av.datetime_partida
            between sfd.faixa_horaria_inicio and sfd.faixa_horaria_fim
        left join
            servico_km_apuracao as s
            on sfd.data = s.data
            and sfd.servico = s.servico
            and s.id_viagem = av.id_viagem
    )
select
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    id_veiculo,
    id_viagem,
    datetime_partida,
    indicador_ar_condicionado,
    indicador_penalidade_judicial,
    indicador_viagem_dentro_limite,
    tipo_viagem,
    tecnologia_apurada,
    tecnologia_remunerada,
    distancia_planejada as km_apurada,
    pof,
    safe_cast(
        coalesce(
            case
                when
                    indicador_viagem_dentro_limite = true
                    and pof >= 80
                    and subsidio_km > 0
                then distancia_planejada
                else 0
            end,
            0
        ) as numeric
    ) as km_subsidiada,
    safe_cast(
        coalesce(
            case
                when indicador_viagem_dentro_limite = true and pof >= 80
                then distancia_planejada * subsidio_km
                else 0
            end,
            0
        ) as numeric
    ) as valor_apurado,
    safe_cast(
        coalesce(valor_glosado_tecnologia, 0) as numeric
    ) as valor_glosado_tecnologia,
    safe_cast(
        coalesce(
            case
                when indicador_viagem_dentro_limite = true
                then 0
                else -1 * (distancia_planejada * subsidio_km)
            end,
            0
        ) as numeric
    ) as valor_acima_limite,
    safe_cast(
        coalesce(
            case
                when pof >= 80 and tipo_viagem != "Não licenciado"
                then distancia_planejada * subsidio_km_teto
                else 0
            end,
            0
        ) as numeric
    ) as valor_sem_glosa,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from subsidio_servico_ar
