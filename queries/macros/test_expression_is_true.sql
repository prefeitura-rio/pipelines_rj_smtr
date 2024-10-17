{% test expression_is_true(model, expression) -%}
SELECT
    *
FROM
    {{ model }}
WHERE
    (DATA BETWEEN DATE("{{ var('start_date') }}")
    AND DATE("{{ var('end_date') }}"))
    AND NOT({{ expression }})
{%- endtest %}