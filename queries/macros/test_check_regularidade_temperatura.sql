{% test test_check_regularidade_temperatura(model) %}

with
    indicadores as (
        select
            data,
            id_veiculo,
            id_viagem,
            ano_fabricacao,
            json_value(indicadores, '$.indicador_regularidade_ar_condicionado.valor') as indicador_regularidade_ar_condicionado,
            json_value(indicadores, '$.indicador_falha_recorrente.valor') as indicador_falha_recorrente,
            json_value(indicadores, '$.indicador_temperatura_descartada.valor') as indicador_temperatura_descartada,
            json_value(indicadores, '$.indicador_temperatura_transmitida.valor') as indicador_temperatura_transmitida,
            json_value(indicadores, '$.indicador_ar_condicionado.valor') as indicador_ar_condicionado,
            json_value(indicadores, '$.indicador_temperatura_regular.valor') as indicador_temperatura_regular,
            json_value(indicadores, '$.indicador_regularidade_ar_condicionado.valor') as indicador_regularidade_ar_condicionado
        from {{ model }}
    ),
    teste_1_falha as (
        select
            data,
            id_veiculo,
            id_viagem,
            ano_fabricacao,
            'Quando deveria `indicador_regularidade_ar_condicionado` ser TRUE, mas não é' as falha
        from indicadores
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
        from indicadores
        where
            (
                -- Quando `indicador_regularidade_ar_condicionado` deveria ser FALSE, mas não é
                (ano_fabricacao <= 2019 or data >= date("{{ var('DATA_SUBSIDIO_V19_INICIO') }}"))
                and (
                    or indicador_falha_recorrente
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
        from indicadores
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