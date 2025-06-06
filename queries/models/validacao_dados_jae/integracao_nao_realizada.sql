{{
    config(
        materilized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set transacao_table = ref("transacao") %}
{% if execute %}
    {% if is_incremental() %}

        {% set partitions_query %}
      SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
      FROM
        `{{ transacao_table.database }}.{{ transacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        {# `rj-smtr.br_rj_riodejaneiro_bilhetagem.INFORMATION_SCHEMA.PARTITIONS` #}
      WHERE
        table_name = "{{ transacao_table.identifier }}"
        AND partition_id != "__NULL__"
        AND DATE(last_modified_time, "America/Sao_Paulo") BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
        {% endset %}

        {{ log("Running query: \n" ~ partitions_query, info=True) }}
        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
        {{ log("transacao partitions: \n" ~ partition_list, info=True) }}
    {% endif %}
{% endif %}

{% set max_transactions = var("quantidade_integracoes_max") %}  -- Número máximo de pernas em uma integração
{% set pivot_columns = [
    "datetime_transacao",
    "id_transacao",
    "modo",
    "servico_sentido",
] %}
{% set transaction_date_filter %}
  {% if partition_list|length > 0 %}
    {% for p in partition_list %}
      (
        data BETWEEN DATE_SUB(DATE({{ p }}), INTERVAL 1 DAY) AND DATE_ADD(DATE({{ p }}), INTERVAL 1 DAY)
        AND
          (
            DATE(datetime_transacao) = DATE({{ p }})
            OR DATE(DATETIME_SUB(datetime_transacao, INTERVAL 180 MINUTE)) = DATE({{ p }})
            OR DATE(DATETIME_ADD(datetime_transacao, INTERVAL 180 MINUTE)) = DATE({{ p }})
          )
      )
      {% if not loop.last %}OR{% endif %}
    {% endfor %}
  {% else %}
    data = "2000-01-01"
  {% endif %}
{% endset %}

with
    matriz as (
        select
            data_inicio_matriz,
            data_fim_matriz,
            array_agg(array_to_string(sequencia_completa_modo, ',')) as sequencia_valida
        from {{ ref("matriz_integracao") }}
        -- `rj-smtr.br_rj_riodejaneiro_bilhetagem.matriz_integracao`
        group by data_inicio_matriz, data_fim_matriz
    ),
    transacao as (
        select
            t.id_cliente,
            {% for column in pivot_columns %}
                {% if column == "servico_sentido" %}
                    concat(t.id_servico_jae, '_', t.sentido) as servico_sentido,
                {% elif column == "modo" %}
                    case
                        when t.modo = 'Van'
                        then t.consorcio
                        when t.modo = 'Ônibus'
                        then 'SPPO'
                        else t.modo
                    end as modo,
                {% else %} {{ column }},
                {% endif %}
            {% endfor %}
            m.data_inicio_matriz,
            m.sequencia_valida
        from {{ ref("transacao") }} t
        {# from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao` t #}
        join
            matriz m
            on t.data >= m.data_inicio_matriz
            and (t.data <= m.data_fim_matriz or m.data_fim_matriz is null)
        where
            t.data < current_date("America/Sao_Paulo")
            and t.tipo_transacao != "Gratuidade"
            and t.id_cliente is not null
            and t.id_cliente != '733'
            {% if is_incremental() %} and ({{ transaction_date_filter }}) {% endif %}
    ),
    transacao_agrupada as (
        select
            id_cliente,
            -- Cria o conjunto de colunas para a transação atual e as 4 próximas
            -- transações do cliente
            {% for column in pivot_columns %}
                {% for transaction_number in range(max_transactions) %}
                    {% if loop.first %}
                        {{ column }} as {{ column }}_{{ transaction_number }},
                    {% else %}
                        lead({{ column }}, {{ loop.index0 }}) over (
                            partition by id_cliente order by datetime_transacao
                        ) as {{ column }}_{{ transaction_number }},
                    {% endif %}
                {% endfor %}
            {% endfor %}
            data_inicio_matriz,
            sequencia_valida
        from transacao
    ),
    integracao_possivel as (
        select
            *,
            {% set modos = ["modo_0"] %}
            {% set servicos = ["servico_sentido_0"] %}
            {% for transaction_number in range(1, max_transactions) %}
                {% do modos.append("modo_" ~ transaction_number) %}
                (
                    datetime_diff(
                        datetime_transacao_{{ transaction_number }},
                        datetime_transacao_0,
                        minute
                    )
                    <= 180
                    and concat({{ modos | join(", ',', ") }})
                    in unnest(sequencia_valida)
                    {% if loop.first %}
                        and servico_sentido_{{ transaction_number }}
                        != servico_sentido_0
                    {% else %}
                        and servico_sentido_{{ transaction_number }}
                        not in ({{ servicos | join(", ',', ") }})
                    {% endif %}
                ) as indicador_integracao_{{ transaction_number }},
                {% do servicos.append("servico_sentido_" ~ transaction_number) %}

            {% endfor %}
        from transacao_agrupada
        where id_transacao_1 is not null
    ),
    transacao_filtrada as (
        select
            id_cliente,
            {% for column in pivot_columns %}
                {% for transaction_number in range(max_transactions) %}
                    {% if transaction_number < 2 %}
                        {{ column }}_{{ transaction_number }},
                    {% else %}
                        case
                            when indicador_integracao_{{ transaction_number }}
                            then {{ column }}_{{ transaction_number }}
                        end as {{ column }}_{{ transaction_number }},
                    {% endif %}
                {% endfor %}
            {% endfor %}
            indicador_integracao_1,
            indicador_integracao_2,
            indicador_integracao_3,
            indicador_integracao_4,
            data_inicio_matriz
        from integracao_possivel
        where indicador_integracao_1
    ),
    transacao_listada as (
        select
            *,
            array_to_string(
                [
                    {% for i in range(1, max_transactions) %}
                        id_transacao_{{ i }} {% if not loop.last %},{% endif %}
                    {% endfor %}
                ],
                ", "
            ) as transacoes
        from transacao_filtrada
    ),
    {% for i in range(max_transactions - 1) %}
        validacao_integracao_{{ max_transactions - i }}_pernas as (
            select
                (
                    id_transacao_{{ max_transactions - i - 1 }} is not null
                    {% if not loop.first %}
                        and id_transacao_{{ max_transactions - i }} is null
                    {% endif %}
                    and id_transacao_0 in unnest(
                        split(
                            string_agg(transacoes, ", ") over (
                                partition by id_cliente
                                order by datetime_transacao_0
                                rows between 5 preceding and 1 preceding
                            ),
                            ', '
                        )
                    )
                ) as remover_{{ max_transactions - i }},
                *
            from {% if loop.first %} transacao_listada
            {% else %}
                    validacao_integracao_{{ max_transactions - i + 1 }}_pernas
                where not remover_{{ max_transactions - i + 1 }}
            {% endif %}
        ),
    {% endfor %}
    integracoes_validas as (
        select date(datetime_transacao_0) as data, id_transacao_0 as id_integracao, *
        from validacao_integracao_2_pernas
        where not remover_2
    ),
    melted as (
        select
            data,
            id_integracao,
            sequencia_integracao,
            datetime_transacao,
            id_transacao,
            modo,
            split(servico_sentido, '_')[0] as id_servico_jae,
            split(servico_sentido, '_')[1] as sentido,
            countif(modo = "BRT") over (partition by id_integracao)
            > 1 as indicador_transferencia_brt,
            countif(modo = "VLT") over (partition by id_integracao)
            > 1 as indicador_transferencia_vlt,
            data_inicio_matriz
        from
            integracoes_validas,
            unnest(
                [
                    {% for transaction_number in range(max_transactions) %}
                        struct(
                            {% for column in pivot_columns %}
                                {{ column }}_{{ transaction_number }} as {{ column }},
                            {% endfor %}
                            {{ transaction_number + 1 }} as sequencia_integracao
                        )
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
                ]
            )
    ),
    integracao_nao_realizada as (
        select distinct id_integracao
        from melted
        where
            not indicador_transferencia_brt
            and not indicador_transferencia_vlt
            and id_transacao not in (
                select id_transacao
                from {{ ref("integracao") }}
                -- `rj-smtr.br_rj_riodejaneiro_bilhetagem.integracao`
                {% if is_incremental() %}
                    where {{ transaction_date_filter }}
                {% endif %}
            )
    )
select
    * except (indicador_transferencia_brt, indicador_transferencia_vlt),
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from melted
where
    id_integracao in (select id_integracao from integracao_nao_realizada)
    and id_transacao is not null
