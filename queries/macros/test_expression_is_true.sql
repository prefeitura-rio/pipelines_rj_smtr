{% test expression_is_true(model, expression) -%}
SELECT
    *
FROM
    {{ model }}
WHERE
    (DATA BETWEEN DATE("{{ var('start_timestamp') }}")
    AND DATE("{{ var('end_timestamp') }}"))
    AND NOT({{ expression }})
{%- endtest %}