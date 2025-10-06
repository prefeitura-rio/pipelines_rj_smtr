{{ config(materialized="ephemeral") }}

with
    subsidio_faixa_dia as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            pof
        from {{ ref("percentual_operacao_faixa_horaria") }}
        -- from `rj-smtr.subsidio.percentual_operacao_faixa_horaria`
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
    ),
    indicador_ar as (
        select
            data,
            id_veiculo,
            status,
            case
                when data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
                then
                    safe_cast(
                        json_value(indicadores, "$.indicador_ar_condicionado") as bool
                    )
                else
                    safe_cast(
                        json_value(
                            indicadores, "$.indicador_ar_condicionado.valor"
                        ) as bool
                    )
            end as indicador_ar_condicionado
        from {{ ref("aux_veiculo_dia_consolidada") }}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    viagem as (
        select
            data, servico_realizado as servico, id_veiculo, id_viagem, datetime_partida
        from {{ ref("viagem_completa") }}
        -- from `rj-smtr.projeto_subsidio_sppo.viagem_completa`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    ar_viagem as (
        select
            v.data,
            v.servico,
            v.id_viagem,
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
            coalesce(s.tipo_viagem, "Sem viagem apurada") as tipo_viagem,
            s.tecnologia_apurada,
            s.tecnologia_remunerada,
            s.id_viagem,
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
    modo,
    indicador_ar_condicionado,
    indicador_penalidade_judicial,
    indicador_viagem_dentro_limite,
    tipo_viagem,
    tecnologia_apurada,
    tecnologia_remunerada,
    coalesce(count(id_viagem), 0) as viagens_faixa,
    coalesce(sum(km_apurada), 0) as km_apurada_faixa,
    coalesce(sum(km_subsidiada), 0) as km_subsidiada_faixa,
    coalesce(sum(valor_apurado), 0) as valor_apurado,
    coalesce(sum(valor_glosado_tecnologia), 0) as valor_glosado_tecnologia,
    coalesce(sum(valor_acima_limite), 0) as valor_acima_limite,
    coalesce(sum(valor_sem_glosa), 0) as valor_total_sem_glosa,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_remunerada
group by
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    modo,
    indicador_ar_condicionado,
    indicador_penalidade_judicial,
    indicador_viagem_dentro_limite,
    tipo_viagem,
    tecnologia_apurada,
    tecnologia_remunerada
