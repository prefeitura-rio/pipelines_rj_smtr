{{
  config(
    materilized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": "data",
      "data_type": "date",
      "granularity": "day"
    },
  )
}}

{% set transacao_table = ref('transacao') %}
{% if execute %}
  {% if is_incremental() %}

    {% set partitions_query %}
      SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
      FROM
        `{{ transacao_table.database }}.{{ transacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        table_name = "{{ transacao_table.identifier }}"
        AND partition_id != "__NULL__"
        AND DATE(last_modified_time, "America/Sao_Paulo") >= DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 1 DAY)
    {% endset %}

    {{ log("Running query: \n"~partitions_query, info=True) }}
    {% set partitions = run_query(partitions_query) %}

    {% set partition_list = partitions.columns[0].values() %}
    {{ log("trasacao partitions: \n"~partition_list, info=True) }}
  {% endif %}
{% endif %}

{% set max_transactions = var('quantidade_integracoes_max') %} -- Número máximo de pernas em uma integração
{% set pivot_columns = ["datetime_transacao", "id_transacao", "modo", "servico_sentido"] %}
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

WITH matriz AS (
  SELECT
    string_agg(modo order by sequencia_integracao) AS sequencia_valida,
    count(*) AS quantidade_transacao
  FROM
    -- ref("matriz_integracao")
    `rj-smtr.br_rj_riodejaneiro_bilhetagem.matriz_integracao`
  group by id_matriz_integracao
),
transacao AS (
  SELECT
    id_cliente,
    {% for column in pivot_columns %}
      {% if column == "servico_sentido" %}
        CONCAT(id_servico_jae, '_', sentido) AS servico_sentido,
      {% else %}
        {{ column }},
      {% endif %}
    {% endfor %}
  FROM
    -- {{ ref("transacao") }}
    `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao`
  WHERE
    data < CURRENT_DATE("America/Sao_Paulo")
    {% if is_incremental() %}
      AND
      {{ transaction_date_filter }}
    {% endif %}
),
transacao_agrupada AS (
  SELECT
    id_cliente,
    -- Cria o conjunto de colunas para a transação atual e as 4 próximas transações do cliente
    {% for column in pivot_columns %}
      {% for transaction_number in range(max_transactions) %}
        {% if loop.first %}
          {{ column }} AS {{ column }}_{{ transaction_number }},
        {% else %}
          LEAD({{ column }}, {{ loop.index0 }}) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS {{ column }}_{{ transaction_number }},
        {% endif %}
      {% endfor %}
    {% endfor %}
  FROM
    transacao
),
integracao_possivel AS (
  SELECT
    *,
    {% set modos = ["modo_0"] %}
    {% set servicos = ["servico_sentido_0"] %}
    {% for transaction_number in range(1, max_transactions) %}
      {% do modos.append("modo_"~transaction_number) %}
      (
        DATETIME_DIFF(datetime_transacao_{{ transaction_number }}, datetime_transacao_0, MINUTE) <= 180
        AND CONCAT({{ modos|join(", ',', ") }}) IN (SELECT sequencia_valida FROM matriz)
        {% if loop.first %}
          AND servico_sentido_{{ transaction_number }} != servico_sentido_0
        {% else %}
          AND servico_sentido_{{ transaction_number }} NOT IN ({{ servicos|join(", ',', ") }})
        {% endif %}
      ) AS indicador_integracao_{{ transaction_number }},
      {% do servicos.append("servico_sentido_"~transaction_number) %}

    {% endfor %}
  FROM
    transacao_agrupada
  WHERE
    id_transacao_1 IS NOT NULL
),
transacao_filtrada AS (
  SELECT
    id_cliente,
    {% for column in pivot_columns %}
      {% for transaction_number in range(max_transactions) %}
        {% if transaction_number < 2 %}
          {{ column }}_{{ transaction_number }},
        {% else %}
          CASE
            WHEN
              indicador_integracao_{{ transaction_number }} THEN {{ column }}_{{ transaction_number }}
            END AS {{ column }}_{{ transaction_number }},
        {% endif %}
      {% endfor %}
    {% endfor %}
    indicador_integracao_1,
    indicador_integracao_2,
    indicador_integracao_3,
    indicador_integracao_4
  FROM
    integracao_possivel
  WHERE
    indicador_integracao_1
),
transacao_listada AS (
  SELECT
    *,
    ARRAY_TO_STRING(
      [
        {% for i in range(1, max_transactions) %}
          id_transacao_{{ i }} {% if not loop.last %},{% endif %}
        {% endfor %}
      ],
      ", "
    ) AS transacoes
  FROM
    transacao_filtrada
),
{% for i in range(max_transactions - 1) %}
  validacao_integracao_{{ max_transactions - i }}_pernas AS (
    SELECT
      (
        id_transacao_{{ max_transactions - i - 1 }} IS NOT NULL
        {% if not loop.first %}
          AND id_transacao_{{ max_transactions - i }} IS NULL
        {% endif %}
        AND id_transacao_0 IN UNNEST(
          SPLIT(
            STRING_AGG(transacoes, ", ") OVER (PARTITION BY id_cliente ORDER BY datetime_transacao_0 ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),
            ', ')
        )
      ) AS remover_{{ max_transactions - i }},
      *
    FROM
      {% if loop.first %}
        transacao_listada
      {% else %}
        validacao_integracao_{{ max_transactions - i + 1 }}_pernas
        WHERE
          NOT remover_{{ max_transactions - i + 1 }}
      {% endif %}
  ),
{% endfor %}
integracoes_validas AS (
  SELECT
    DATE(datetime_transacao_0) AS data,
    GENERATE_UUID() AS id_integracao,
    *
  FROM
    validacao_integracao_2_pernas
  WHERE
    NOT remover_2
),
melted AS (
  SELECT
    data,
    GENERATE_UUID() AS id_integracao,
    sequencia_integracao,
    datetime_transacao,
    id_transacao,
    id_cliente,
    modo,
    SPLIT(servico_sentido, '_')[0] AS id_servico_jae,
    SPLIT(servico_sentido, '_')[1] AS sentido
  FROM
    integracoes_validas,
    UNNEST(
      [
        {% for transaction_number in range(max_transactions) %}
          STRUCT(
            {% for column in pivot_columns %}
              {{ column }}_{{ transaction_number }} AS {{ column }},
            {% endfor %}
            {{ transaction_number }} AS sequencia_integracao
          ){%if not loop.last %},{% endif %}
      {% endfor %}
      ]
    )
),
integracao_nao_realizada AS (
  SELECT DISTINCT
    id_integracao
  FROM
    melted
  WHERE
    id_transacao NOT IN (
      SELECT
        id_transacao
      FROM
        -- ref("integracao")
        `rj-smtr.br_rj_riodejaneiro_bilhetagem.integracao`
      {% if is_incremental() %}
        WHERE
          {{ transaction_date_filter }}
      {% endif %}
    )
)
-- SELECT
--   *,
--   '{{ var("version") }}' as versao
-- FROM
--   melted
-- WHERE
--   id_integracao IN (SELECT id_integracao FROM integracao_nao_realizada)
SELECT CURRENT_DATE() AS data, * FROM integracao_nao_realizada