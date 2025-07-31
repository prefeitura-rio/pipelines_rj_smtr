{% test test_check_regularidade_temperatura(model) %}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{ var('date_range_end') }}") and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

with
    indicadores_aux_viagem_temperatura as (
        select
            data,
            id_veiculo,
            id_viagem,
            safe_cast(json_value(indicadores, '$.indicador_temperatura_descartada.valor') as bool) as indicador_temperatura_descartada,
            safe_cast(json_value(indicadores, '$.indicador_temperatura_transmitida.valor') as bool) as indicador_temperatura_transmitida,
            safe_cast(json_value(indicadores, '$.indicador_temperatura_regular.valor') as bool) as indicador_temperatura_regular
        from {{ ref("aux_viagem_temperatura") }}
        where {{ incremental_filter }}
    ),
    indicadores_veiculo_regularidade_temperatura_dia as (
        select
            data,
            id_veiculo,
            indicadores.indicador_falha_recorrente.valor as indicador_falha_recorrente
        from {{ ref("veiculo_regularidade_temperatura_dia") }}
        where {{ incremental_filter }}
    ),
    indicadores as (
        select
            data,
            id_veiculo,
            id_viagem,
            ano_fabricacao,
            safe_cast(json_value(indicadores, '$.indicador_ar_condicionado.valor') as bool) as indicador_ar_condicionado,
            safe_cast(json_value(indicadores, '$.indicador_regularidade_ar_condicionado.valor') as bool) as indicador_regularidade_ar_condicionado
        from {{ model }}
    ),
    indicadores_completo as (
        select
            i.data,
            i.id_veiculo,
            i.id_viagem,
            i.ano_fabricacao,
            i.indicador_ar_condicionado,
            i.indicador_regularidade_ar_condicionado,
            t.indicador_temperatura_descartada,
            t.indicador_temperatura_transmitida,
            t.indicador_temperatura_regular,
            v.indicador_falha_recorrente
        from indicadores i
        left join indicadores_aux_viagem_temperatura t using (data, id_veiculo, id_viagem)
        left join indicadores_veiculo_regularidade_temperatura_dia v using (data, id_veiculo)
    ),
    teste_1_falha as (
        select
            data,
            id_veiculo,
            id_viagem,
            ano_fabricacao,
            'Quando deveria `indicador_regularidade_ar_condicionado` ser TRUE, mas não é' as falha
        from indicadores_completo
        where
            (
                -- Quando deveria `indicador_regularidade_ar_condicionado` ser TRUE, mas não é
                (ano_fabricacao <= 2019
                    or  data >= date("{{ var('DATA_SUBSIDIO_V19_INICIO') }}"))
                and indicador_ar_condicionado
                and not indicador_falha_recorrente
                and not indicador_temperatura_descartada
                and indicador_temperatura_transmitida
                and indicador_temperatura_regular
                and not indicador_regularidade_ar_condicionado
            )
            ),
    teste_2_falha as (
        select
            data,
            id_veiculo,
            id_viagem,
            ano_fabricacao,
            'Quando `indicador_regularidade_ar_condicionado` deveria ser FALSE, mas não é' as falha
        from indicadores_completo
        where
            (
                -- Quando `indicador_regularidade_ar_condicionado` deveria ser FALSE, mas não é
                (ano_fabricacao <= 2019
                 or data >= date("{{ var('DATA_SUBSIDIO_V19_INICIO') }}"))
                and (
                    indicador_falha_recorrente
                    or indicador_temperatura_descartada
                    or not indicador_temperatura_transmitida
                    or not indicador_temperatura_regular
                )
                and indicador_regularidade_ar_condicionado
            )
            ),
    teste_3_falha as (
        select
            data,
            id_veiculo,
            id_viagem,
            ano_fabricacao,
            'Quando deveria ser NULL de acordo com o Art 2º-E da resolução, mas não é' as falha
        from indicadores_completo
        where
            (
                -- Quando deveria ser NULL de acordo com o Art 2º-E da resolução, mas não é
                (ano_fabricacao > 2019
                and data <= date("{{ var('DATA_SUBSIDIO_V19_INICIO') }}"))
                and not indicador_ar_condicionado
                and indicador_regularidade_ar_condicionado is not null
            )
            ),
    falhas as (
        select data, id_veiculo, id_viagem, ano_fabricacao, falha
        from teste_1_falha

        union all

        select data, id_veiculo, id_viagem, ano_fabricacao, falha
        from teste_2_falha

        union all

        select data, id_veiculo, id_viagem, ano_fabricacao, falha
        from teste_3_falha
    )
    select *
    from falhas
{% endtest %}
