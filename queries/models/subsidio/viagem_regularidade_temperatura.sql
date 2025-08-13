{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('start_date')}}") and date("{{var('end_date')}}") and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

with
    viagem_temperatura as (
        select
            data,
            id_viagem,
            id_veiculo,
            datetime_partida,
            datetime_chegada,
            modo,
            ano_fabricacao,
            tecnologia_apurada,
            tecnologia_remunerada,
            tipo_viagem,
            indicadores,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_variacao.valor'
                ) as bool
            ) as indicador_temperatura_variacao,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_transmitida.valor'
                ) as bool
            ) as indicador_temperatura_transmitida,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_descartada.valor'
                ) as bool
            ) as indicador_temperatura_descartada,
            safe_cast(
                json_value(indicadores, '$.indicador_temperatura_regular.valor') as bool
            ) as indicador_temperatura_regular,
            servico,
            sentido,
            distancia_planejada
        from {{ ref("aux_viagem_temperatura") }}
        where {{ incremental_filter }}
    ),
    veiculo_regularidade as (
        select
            data,
            id_veiculo,
            indicadores.indicador_falha_recorrente.valor as indicador_falha_recorrente
        from {{ ref("veiculo_regularidade_temperatura_dia") }}
        where {{ incremental_filter }}
    ),
    regularidade_temperatura as (
        select
            data,
            id_viagem,
            id_veiculo,
            datetime_partida,
            datetime_chegada,
            modo,
            ano_fabricacao,
            tecnologia_apurada,
            tecnologia_remunerada,
            case
                when
                    vt.tipo_viagem not in (
                        "Licenciado com ar e não autuado",
                        "Licenciado sem ar e não autuado"
                    )
                then vt.tipo_viagem
                when
                    (
                        ano_fabricacao <= 2019
                        and (
                            coalesce(vr.indicador_falha_recorrente, false)
                            or vt.indicador_temperatura_descartada
                            or not vt.indicador_temperatura_transmitida
                            or not vt.indicador_temperatura_regular
                        )
                        and data >= date('{{ var("DATA_SUBSIDIO_V17_INICIO") }}')
                    )
                    or (
                        (
                            coalesce(vr.indicador_falha_recorrente, false)
                            or vt.indicador_temperatura_descartada
                            or not vt.indicador_temperatura_transmitida
                            or not vt.indicador_temperatura_regular
                        )
                        and data >= date('{{ var("DATA_SUBSIDIO_V19_INICIO") }}')
                    )
                then "Detectado com ar inoperante"
                else vt.tipo_viagem
            end as tipo_viagem,
            case
                when
                    (
                        vt.ano_fabricacao <= 2019
                        or vt.data >= date('{{ var("DATA_SUBSIDIO_V19_INICIO") }}')
                    )
                then
                    (
                        not coalesce(vr.indicador_falha_recorrente, false)
                        and not vt.indicador_temperatura_descartada
                        and vt.indicador_temperatura_transmitida
                        and vt.indicador_temperatura_regular
                    )
                else null
            end as indicador_regularidade_ar_condicionado,
            indicadores,
            servico,
            sentido,
            distancia_planejada
        from viagem_temperatura as vt
        left join veiculo_regularidade as vr using (data, id_veiculo)
    )
select
    data,
    id_viagem,
    id_veiculo,
    datetime_partida,
    datetime_chegada,
    modo,
    ano_fabricacao,
    tecnologia_apurada,
    tecnologia_remunerada,
    tipo_viagem,
    json_set(
        json_set(
            indicadores,
            '$.indicador_regularidade_ar_condicionado.valor',
            indicador_regularidade_ar_condicionado
        ),
        '$.indicador_regularidade_ar_condicionado.datetime_apuracao_subsidio',
        current_datetime("America/Sao_Paulo")
    ) as indicadores,
    servico,
    sentido,
    distancia_planejada,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from regularidade_temperatura
