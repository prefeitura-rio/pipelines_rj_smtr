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
    servico_km_apuracao as (
        select *
        from {{ ref("viagem_conformidade_limite") }}
        {# from `rj-smtr-dev.botelho__subsidio.viagem_conformidade_limite` #}
        where {{ incremental_filter }}
    ),
    indicador_ar as (
        select
            data,
            id_veiculo,
            safe_cast(
                json_value(indicadores, "$.indicador_ar_condicionado") as bool
            ) as indicador_ar_condicionado
        from {{ ref("veiculo_dia") }}
        {# from `rj-smtr.monitoramento.veiculo_dia` #}
        where {{ incremental_filter }}
    ),
    subsidio_servico_ar as (
        select
            s.data,
            s.tipo_dia,
            s.subtipo_dia,
            s.faixa_horaria_inicio,
            s.faixa_horaria_fim,
            s.consorcio,
            s.servico,
            s.sentido,
            s.modo,
            s.pof,
            s.id_veiculo,
            coalesce(s.tipo_viagem, "Sem viagem apurada") as tipo_viagem,
            s.tecnologia_apurada,
            s.tecnologia_remunerada,
            s.id_viagem,
            s.datetime_partida,
            safe_cast(s.distancia_planejada as numeric) as distancia_planejada,
            safe_cast(s.subsidio_km as numeric) as subsidio_km,
            safe_cast(s.subsidio_km_teto as numeric) as subsidio_km_teto,
            coalesce(s.valor_glosado_tecnologia, 0) as valor_glosado_tecnologia,
            s.indicador_viagem_dentro_limite,
            s.indicador_penalidade_tecnologia,
            case
                when s.pof < 60 then true else s.indicador_penalidade_judicial
            end as indicador_penalidade_judicial,
            coalesce(ia.indicador_ar_condicionado, false) as indicador_ar_condicionado
        from servico_km_apuracao as s
        left join
            indicador_ar as ia on s.data = ia.data and s.id_veiculo = ia.id_veiculo
    )
select
    data,
    tipo_dia,
    subtipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    sentido,
    modo,
    id_veiculo,
    id_viagem,
    datetime_partida,
    indicador_ar_condicionado,
    indicador_penalidade_judicial,
    indicador_penalidade_tecnologia,
    indicador_viagem_dentro_limite,
    tipo_viagem,
    tecnologia_apurada,
    tecnologia_remunerada,
    distancia_planejada as km_apurada,
    subsidio_km,
    subsidio_km_teto,
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
        (
            case
                when pof >= 80 and tipo_viagem != "Não licenciado"
                then distancia_planejada * subsidio_km_teto
                else 0
            end
        ) - coalesce(
            (
                case
                    when indicador_viagem_dentro_limite = true
                    then 0
                    else distancia_planejada * subsidio_km
                end
            ),
            0
        ) as numeric
    ) as valor_sem_glosa,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from subsidio_servico_ar
