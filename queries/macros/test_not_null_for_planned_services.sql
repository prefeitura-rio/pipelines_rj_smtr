{% test not_null_for_planned_services(model, column_name) -%}
WITH servicos_validos AS (
    SELECT DISTINCT servico
    FROM {{ ref('viagem_planejada') }}
    WHERE data BETWEEN "{{ var('date_range_start') }}" AND "{{ var('date_range_end') }}"
)

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NULL
  AND servico IN (SELECT servico FROM servicos_validos)
{% endtest %}