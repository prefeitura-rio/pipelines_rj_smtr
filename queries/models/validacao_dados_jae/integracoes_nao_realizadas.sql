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
        AND DATE(last_modified_time, "America/Sao_Paulo") = DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 1 DAY)
    {% endset %}

    {{ log("Running query: \n"~partitions_query, info=True) }}
    {% set partitions = run_query(partitions_query) %}

    {% set partition_list = partitions.columns[0].values() %}
    {{ log("trasacao partitions: \n"~partition_list, info=True) }}
  {% endif %}
{% endif %}

{% set max_transactions = 5 %} -- Número máximo de pernas em uma integração
{% set pivot_columns = ["datetime_transacao", "id_transacao", "modo", "servico_sentido"] %}
WITH matriz AS (
  SELECT
    string_agg(modo order by sequencia_integracao) AS sequencia_valida,
    count(*) AS quantidade_transacao
  FROM
    rj-smtr.br_rj_riodejaneiro_bilhetagem.matriz_integracao
  group by id_matriz_integracao
),
transacao AS (
  SELECT
    id_cliente,
    {% for column in pivot_columns %}
      {% if column == "servico_sentido" %}
        CONCAT(id_servico_jae, '_', sentido),
      {% else %}
        {{ column }},
      {% endif %}
    {% endfor %}
  FROM
    {{ ref("transacao") }}
  WHERE
    DATA = '2024-07-23'
    and id_cliente IN ('243053', '235930')
    -- {% if is_incremental() %}
    --   {% if partition_list|length > 0 %}
    --     data IN ({{ partition_list|join(', ') }})
    --   {% else %}
    --     data = "2000-01-01"
    --   {% endif %}
    --   {% else %}
    --     data < CURRENT_DATE("America/Sao_Paulo")
    -- {% endif %}
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
          LEAD({{ column }}) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS {{ column }}_{{ transaction_number }},
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
    {% set servicos = [] %}
    {% for transaction_number in range(1, max_transactions) %}
      {{ modos.append("modo_"~transaction_number) }}
      (
        DATETIME_DIFF(datetime_transacao_{ transaction_number }, datetime_transacao_0, MINUTE) <= 180
        AND CONCAT({{ modos|join(", ',', ") }}) IN (SELECT sequencia_valida FROM matriz)
        {% if loop.first %}
          AND servico_{{ transaction_number }} != servico_0
        {% else %}
          AND servico_{{ transaction_number }} NOT IN ({{ servicos|join(", ',', ") }})
        {% endif %}
      ) AS indicador_integracao_{{ transaction_number }},
      {{ servicos.append("servicos_"~transaction_number) }}

    {% endfor %}
  FROM
    agrupada
  WHERE
    id_transacao_1 IS NOT NULL
),
transacao_filtrada AS (
  SELECT
    id_cliente,
    {% for column in pivot_columns %}
      {% for transaction_number in range(max_transactions) %}
        {% if transaction_number < 2 %}
          {{ column }}_transaction_number,
        {% else %}
          CASE
            WHEN
              indicador_integracao_{{ transaction_number }} THEN column_{{ transaction_number }}
            END AS column_{{ transaction_number }},
        {% endif %}
      {% endfor %}
    {% endfor %}
    indicador_integracao_1,
    indicador_integracao_2,
    indicador_integracao_3,
    indicador_integracao_4
  FROM
    integracao
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
    campos_tratados
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
  ){% if not loop.last %},{% endif %}
{% endfor %}
SELECT
  *
FROM
  validacao_integracao_2_pernas
WHERE
  NOT remover_2