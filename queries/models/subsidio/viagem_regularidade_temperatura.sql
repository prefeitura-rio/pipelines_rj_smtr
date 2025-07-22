{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('start_date')}}") and date("{{var('end_date')}}") and data >= date("{{ var('DATA_SUBSIDIO_V16_INICIO') }}")
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
            (
                (
                    ano_fabricacao <= 2019
                    and indicador_falha
                    and data >= date('{{ var("DATA_SUBSIDIO_V16_INICIO") }}')
                )
                or (
                    indicador_falha
                    and data >= date('{{ var("DATA_SUBSIDIO_V18_INICIO") }}')
                )
            ) as indicador_falha
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
                when vr.indicador_falha
                then "Detectado com ar inoperante"
                else vt.tipo_viagem
            end as tipo_viagem,
            case
                when
                    (
                        vt.ano_fabricacao <= 2019
                        or vt.data >= date('{{ var("DATA_SUBSIDIO_V18_INICIO") }}')
                    )
                then vr.indicador_falha
                else null
            end as indicador_regularidade_temperatura,
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
            '$.indicador_regularidade_temperatura.valor',
            indicador_regularidade_temperatura
        ),
        '$.indicador_regularidade_temperatura.data_apuracao_subsidio',
        current_datetime("America/Sao_Paulo")
    ) as indicadores,
    servico,
    sentido,
    distancia_planejada,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from regularidade_temperatura
