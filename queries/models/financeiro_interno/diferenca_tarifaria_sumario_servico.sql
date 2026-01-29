{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    diferenca_tarifaria_sumario_servico as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            any_value(irk) as irk,
            sum(viagens_faixa) as viagens_dia,
            sum(km_atendida_faixa) as km_atendida_dia,
            sum(km_conforme_faixa) as km_conforme_dia,
            sum(km_planejada_faixa) as km_planejada_dia,
            sum(receita_tarifa_publica_faixa) as receita_tarifa_publica_dia,
            safe_cast(
                coalesce(
                    round(
                        100 * sum(km_atendida_faixa)
                        / sum(km_planejada_faixa),
                        2
                    ),
                    0
                ) as numeric
            ) as percentual_atendimento_dia,
            sum(valor_penalidade_faixa) as valor_penalidade_dia,
        from
            {# {{ ref("diferenca_tarifaria_sumario_servico_faixa_sentido") }} #}
            `rj-smtr-dev`.`rodrigo__financeiro_interno`.`diferenca_tarifaria_sumario_servico_faixa_sentido`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
        group by all
    )
select
    data,
    tipo_dia,
    consorcio,
    servico,
    irk,
    viagens_dia,
    km_conforme_dia,
    km_planejada_dia,
    percentual_atendimento_dia,
    receita_tarifa_publica_dia,
    valor_penalidade_dia,
    if(
        percentual_atendimento_dia >= 80,
        km_conforme_dia * irk - receita_tarifa_publica_dia,
        0
    )
    + valor_penalidade_dia as delta_tr,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from diferenca_tarifaria_sumario_servico
