{% test unique_combination(model, expression) -%}
SELECT
    {{expression}}
FROM
    {{ model }}
WHERE
    DATA BETWEEN DATE("{{ var('start_date') }}")
    AND DATE("{{ var('end_date') }}")
GROUP BY
    {{expression}}
HAVING
    COUNT(*) > 1
{%- endtest %}