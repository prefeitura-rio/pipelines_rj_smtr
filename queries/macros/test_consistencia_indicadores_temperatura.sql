{% test consistencia_indicadores_temperatura(model) %}

    with
        indicadores as (
            select
                data,
                id_viagem,
                quantidade_pre_tratamento,
                quantidade_nula,
                quantidade_zero,
                quantidade_pos_tratamento,
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
            from `rj-smtr.subsidio_staging.aux_viagem_temperatura`
        ),

        /* countif(temperatura != 0) > 0 as indicador_temperatura_transmitida_viagem, */
        /* Se q quantidade pré-tratamento for zero ou pós for zero, 
então não tem como ter temperatura transmitida como true */
        indicador_temperatura_transmitida_viagem as (
            select *
            from indicadores
            where
                (
                    quantidade_pre_tratamento = 0
                    and indicador_temperatura_transmitida_viagem = true
                )
                or (
                    quantidade_pos_tratamento = 0
                    and indicador_temperatura_transmitida_viagem = true
                )
        ),

        /* count(distinct case when temperatura != 0 then temperatura end) > 1 as indicador_temperatura_variacao_viagem*/
        indicador_temperatura_variacao_viagem as (
            select *
            from indicadores
            where
                (
                    (
                        quantidade_pre_tratamento - quantidade_zero = 1
                        and indicador_temperatura_variacao_viagem = true
                    )  /* só 1 valor dif de 0 mas marcou true, ou seja, a temperatura se repetiu*/
                    or (
                        quantidade_pre_tratamento = quantidade_zero
                        and indicador_temperatura_variacao_viagem = true
                    )  /* Todas as temperaturas ficaram zero*/
                )
        ),

        /* indicador_temperatura_pos_tratamento_descartada_viagem
  percentual_temperatura_pos_tratamento_descartada
            > 50 as indicador_temperatura_pos_tratamento_descartada_viagem,
            percentual_temperatura_zero_descartada
            = 100 as indicador_temperatura_zero_viagem,
            percentual_temperatura_nula_descartada
            = 100 as indicador_temperatura_nula_viagem*/
        temperatura_perc as (
            select *
            from indicadores
            where
                (
                    percentual_temperatura_pos_tratamento_descartada <= 50
                    and indicador_temperatura_pos_tratamento_descartada_viagem = true
                )
                or (
                    percentual_temperatura_zero_descartada < 100
                    and indicador_temperatura_zero_viagem = true
                )
                or (
                    percentual_temperatura_nula_descartada < 100
                    and indicador_temperatura_nula_viagem = true
                )
        )

    select
        i.data,
        i.id_viagem,
        case
            when t.id_viagem is not null then true else false
        end as inconsistencia_temp_transmitida,
        case
            when v.id_viagem is not null then true else false
        end as inconsistencia_temp_variacao,
        case
            when p.id_viagem is not null then true else false
        end as inconsistencia_percentuais

    from indicadores i
    left join indicador_temperatura_transmitida_viagem t using (id_viagem, data)
    left join indicador_temperatura_variacao_viagem v using (id_viagem, data)
    left join temperatura_perc p using (id_viagem, data)
    where t.id_viagem is not null or v.id_viagem is not null or p.id_viagem is not null
{%- endtest %}
