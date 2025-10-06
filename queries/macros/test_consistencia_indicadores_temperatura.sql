{% test consistencia_indicadores_temperatura(model) %}

    with
        indicadores as (
            select
                data,
                id_viagem,
                id_veiculo,
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_validador.id_validador'
                    ) as string
                ) as validador,
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_validador.quantidade_pre_tratamento'
                    ) as numeric
                ) as quantidade_pre_tratamento,
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_validador.quantidade_nula'
                    ) as numeric
                ) as quantidade_nula,
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_validador.quantidade_zero'
                    ) as numeric
                ) as quantidade_zero,
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_validador.quantidade_pos_tratamento'
                    ) as numeric
                ) as quantidade_pos_tratamento,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_temperatura_transmitida_viagem.indicador_temperatura_transmitida_viagem'
                    ) as bool
                ) as indicador_temperatura_transmitida_viagem,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_temperatura_variacao_viagem.indicador_temperatura_variacao_viagem'
                    ) as bool
                ) as indicador_temperatura_variacao_viagem,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_temperatura_menor_igual_24.indicador_temperatura_menor_igual_24'
                    ) as bool
                ) as indicador_temperatura_menor_igual_24,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_temperatura_zero_viagem.indicador_temperatura_zero_viagem'
                    ) as bool
                ) as indicador_temperatura_zero_viagem,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_temperatura_pos_tratamento_descartada_viagem.indicador_temperatura_pos_tratamento_descartada_viagem'
                    ) as bool
                ) as indicador_temperatura_pos_tratamento_descartada_viagem,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_temperatura_nula_viagem.indicador_temperatura_nula_viagem'
                    ) as bool
                ) as indicador_temperatura_nula_viagem,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_validador.percentual_temperatura_nula_descartada'
                    ) as numeric
                ) as percentual_temperatura_nula_descartada,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_validador.percentual_temperatura_pos_tratamento_descartada'
                    ) as numeric
                ) as percentual_temperatura_pos_tratamento_descartada,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_validador.percentual_temperatura_zero_descartada'
                    ) as numeric
                ) as percentual_temperatura_zero_descartada,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_falha_recorrente.indicador_falha_recorrente'
                    ) as bool
                ) as indicador_falha_recorrente

            from `rj-smtr.subsidio.viagem_regularidade_temperatura`
        ),

        indicador_temperatura_transmitida_viagem as (
            select
                *,
                case
                    when
                        quantidade_pre_tratamento = 0
                        and indicador_temperatura_transmitida_viagem = true
                    then
                        "Nenhuma temperatura foi transmitida no indicador_temperatura_transmitida_viagem"
                end as temperatura_nao_transmitida,
                case
                    when
                        quantidade_pre_tratamento = quantidade_zero
                        and indicador_temperatura_transmitida_viagem = true
                    then
                        "Todas temperaturas transmitidas foram iguais a zero no indicador_temperatura_transmitida_viagem"
                end as todas_temperaturas_transmitidas_iguais_zero
            from indicadores
        ),

        indicador_temperatura_variacao_viagem as (
            select
                *,
                case
                    when
                        (
                            quantidade_pre_tratamento = quantidade_nula
                            and indicador_temperatura_variacao_viagem = true
                        )
                    then
                        "Todas temperaturas são nulas no indicador_temperatura_variacao_viagem"
                end as todas_temperaturas_nulas,
                case
                    when
                        (
                            quantidade_pre_tratamento = quantidade_zero
                            and indicador_temperatura_variacao_viagem = true
                        )
                    then
                        "Todas temperaturas iguais a zero no indicador_temperatura_variacao_viagem"
                end as todas_temperaturas_zero
            from indicadores
        ),

        percentuais as (
            select
                *,
                trunc(
                    (
                        1 - coalesce(
                            safe_divide(
                                quantidade_pos_tratamento, quantidade_pre_tratamento
                            ),
                            0
                        )
                    )
                    * 100,
                    2
                ) as test_percentual_temperatura_pos_tratamento_descartada,

                trunc(
                    (
                        1 - coalesce(
                            safe_divide(quantidade_zero, quantidade_pre_tratamento), 0
                        )
                    )
                    * 100,
                    2
                ) as test_percentual_temperatura_zero_descartada,

                trunc(
                    (
                        1 - coalesce(
                            safe_divide(quantidade_nula, quantidade_pre_tratamento), 0
                        )
                    )
                    * 100,
                    2
                ) as test_percentual_temperatura_nula_descartada
            from indicadores
        ),

        teste_percentual as (
            select
                data,
                id_viagem,
                round(
                    p.test_percentual_temperatura_pos_tratamento_descartada
                    - i.percentual_temperatura_pos_tratamento_descartada,
                    2
                ) as diff_percentual_pos,
                round(
                    p.test_percentual_temperatura_zero_descartada
                    - i.percentual_temperatura_zero_descartada,
                    2
                ) as diff_percentual_zero,
                round(
                    p.test_percentual_temperatura_nula_descartada
                    - i.percentual_temperatura_nula_descartada,
                    2
                ) as diff_percentual_nula,
            from percentuais p
            left join indicadores i using (data, id_viagem)
        ),

        indicador_temperatura_pos_tratamento_descartada_viagem as (
            select
                *,
                case
                    when
                        (
                            percentual_temperatura_pos_tratamento_descartada > 50
                            and indicador_temperatura_pos_tratamento_descartada_viagem
                            = true
                        )
                    then
                        "Falha na temperatura descartada pós tratamento no indicador_temperatura_pos_tratamento_descartada_viagem"
                end as inconsistencia_descarte_pos,
                case
                    when
                        (
                            percentual_temperatura_zero_descartada < 100
                            and indicador_temperatura_zero_viagem = true
                        )
                    then
                        "Falha na temperatura zero descartada no indicador_temperatura_zero_viagem "
                end as inconsistencia_temperatura_zero,
                case
                    when
                        (
                            percentual_temperatura_nula_descartada < 100
                            and indicador_temperatura_nula_viagem = true
                        )
                    then
                        "Falha na temperatura nula descartada no indicador_temperatura_zero_viagem "
                end as inconsistencia_temperatura_nula
            from indicadores
        ),

        falha_recorrente as (
            select
                v.data,
                v.id_veiculo,
                v.ano_fabricacao,
                v.quantidade_dia_falha_operacional,
                i.indicador_falha_recorrente,
                i.id_viagem
            from `rj-smtr.monitoramento.veiculo_regularidade_temperatura_dia` v
            left join indicadores i using (data, id_veiculo)
            where v.ano_fabricacao <= 2019
        ),

        analise_falha_recorrente as (
            select
                data,
                id_viagem,
                id_veiculo,
                ano_fabricacao,
                quantidade_dia_falha_operacional,
                indicador_falha_recorrente,
                case
                    when
                        quantidade_dia_falha_operacional < 6
                        and indicador_falha_recorrente = true
                    then 'falha_recorrente'
                end as falha_recorrente_inconsistencia
            from falha_recorrente
        )

    select
        i.data as data,
        i.id_viagem as id_viagem,
        p.diff_percentual_pos as test_dif_percentual_pos_tratamento,
        p.diff_percentual_zero as test_dif_percentual_zero,
        p.diff_percentual_nula as test_dif_percentual_nula,
        tt.temperatura_nao_transmitida as test_temperatura_nao_transmitida,
        tt.todas_temperaturas_transmitidas_iguais_zero as test_todas_temp_iguais_zero,
        vv.todas_temperaturas_nulas as test_todas_temp_nulas,
        vv.todas_temperaturas_zero as test_todas_temp_zero,
        dd.inconsistencia_descarte_pos as test_inconsistencia_descarte_pos,
        dd.inconsistencia_temperatura_zero as test_inconsistencia_temperatura_zero,
        dd.inconsistencia_temperatura_nula as test_inconsistencia_temperatura_nula,
        fr.falha_recorrente_inconsistencia as test_falha_recorrente_inconsistencia

    from indicadores i
    left join teste_percentual p using (data, id_viagem)
    left join indicador_temperatura_transmitida_viagem tt using (data, id_viagem)
    left join indicador_temperatura_variacao_viagem vv using (data, id_viagem)
    left join
        indicador_temperatura_pos_tratamento_descartada_viagem dd using (
            data, id_viagem
        )
    left join analise_falha_recorrente fr using (data, id_viagem)
    where
        p.diff_percentual_pos is not null
        or p.diff_percentual_zero is not null
        or p.diff_percentual_nula is not null
        or tt.temperatura_nao_transmitida is not null
        or tt.todas_temperaturas_transmitidas_iguais_zero is not null
        or vv.todas_temperaturas_nulas is not null
        or vv.todas_temperaturas_zero is not null
        or dd.inconsistencia_descarte_pos is not null
        or dd.inconsistencia_temperatura_zero is not null
        or dd.inconsistencia_temperatura_nula is not null
        or fr.falha_recorrente_inconsistencia is not null
    ;
{%- endtest %}
