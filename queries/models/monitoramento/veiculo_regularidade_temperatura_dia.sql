{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('start_date')}}") and date("{{var('end_date')}}")
            and data >= date("{{ var('DATA_SUBSIDIO_V16_INICIO') }}")
{% endset %}

with
    aux_veiculo as (
        select
            data,
            id_veiculo,
            ano_fabricacao,
            indicador_ar_condicionado,
            data_processamento_licenciamento,
            data_verificacao_regularidade,
            indicador_temperatura_variacao,
            indicador_temperatura_transmitida,
            percentual_temperatura_nula_descartada,
            percentual_temperatura_atipica_descartada,
            indicador_temperatura_descartada,
            quantidade_dia_falha_operacional
        from {{ ref("aux_veiculo_falha_ar_condicionado") }}
        where {{ incremental_filter }} and indicio_falha
    ),
    indicador_motivo as (
        select
            *,
            quantidade_dia_falha_operacional >= 6 as indicador_falha_recorrente,
            case
                when quantidade_dia_falha_operacional > 0
                then
                    (
                        select string_agg(motivo, ', ')
                        from
                            unnest(
                                [
                                    if(
                                        not indicador_temperatura_variacao,
                                        "Repetição do mesmo valor de temperatura ao longo de todas as viagens realizadas em um dia de operação",
                                        ""
                                    ),
                                    if(
                                        not indicador_temperatura_transmitida,
                                        "Ausência total de transmissão de dados de temperatura interna durante um dia de operação",
                                        ""
                                    ),
                                    if(
                                        indicador_temperatura_descartada,
                                        "Descarte de mais de 50% dos registros de temperatura de todas as viagens realizadas em um dia de operação",
                                        ""
                                    )
                                ]
                            ) as motivo
                        where motivo != ""
                    )
                else null
            end as motivo
        from aux_veiculo
    ),
    dados_novos as (
        select
            data,
            id_veiculo,
            ano_fabricacao,
            to_json_string(
                struct(
                    struct(
                        data_processamento_licenciamento,
                        indicador_ar_condicionado as valor
                    ) as indicador_ar_condicionado,
                    struct(
                        data_verificacao_regularidade,
                        indicador_temperatura_variacao as valor
                    ) as indicador_temperatura_variacao,
                    struct(
                        data_verificacao_regularidade,
                        indicador_temperatura_transmitida as valor
                    ) as indicador_temperatura_transmitida,
                    struct(
                        data_verificacao_regularidade,
                        indicador_temperatura_descartada as valor,
                        safe_cast(
                            percentual_temperatura_nula_descartada as string
                        ) as percentual_temperatura_nula_descartada,
                        safe_cast(
                            percentual_temperatura_atipica_descartada as string
                        ) as percentual_temperatura_atipica_descartada
                    ) as indicador_temperatura_descartada,
                    struct(
                        current_date("America/Sao_Paulo") as data_verificacao_falha,
                        indicador_falha_recorrente as valor
                    ) as indicador_falha_recorrente
                )
            ) as indicadores,
            quantidade_dia_falha_operacional,
            motivo,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            "{{ var('version') }}" as versao,
            '{{ invocation_id }}' as id_execucao_dbt
        from indicador_motivo
    )
{% if is_incremental() %}
        ,
        dados_completos as (
            select *, 1 as ordem
            from dados_novos

            union all

            select *, 0 as ordem
            from {{ this }}
            where {{ incremental_filter }}

        ),
        sha_dados as (
            {% set columns = (
                list_columns()
                | reject(
                    "in",
                    [
                        "versao",
                        "datetime_ultima_atualizacao",
                        "id_execucao_dbt",
                    ],
                )
                | list
            ) %}

            select
                *,
                sha256(
                    concat(
                        {% for c in columns %}
                            ifnull(
                                {% if c == "indicadores" %}to_json_string(indicadores)
                                {% else %}cast({{ c }} as string)
                                {% endif %},
                                'n/a'
                            )
                            {% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
                ) as sha_dado
            from dados_completos
        ),
        dados_completos_invocation_id as (
            select
                * except (id_execucao_dbt, sha_dado),
                case
                    when
                        lag(sha_dado) over (win) != sha_dado
                        or (lag(sha_dado) over (win) is null and ordem = 1)
                    then id_execucao_dbt
                    else lag(id_execucao_dbt) over (win)
                end as id_execucao_dbt
            from sha_dados
            window win as (partition by data, id_veiculo order by ordem)
        )

    select * except (ordem)
    from dados_completos_invocation_id
    where ordem = 1
{% else %} select * from dados_novos
{% endif %}

    -- trocar variaveis dbt de acordo com o flow
    -- materializar veiculo_dia todo dia

