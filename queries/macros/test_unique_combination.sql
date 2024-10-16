{% test unique_combination(model, expression) -%}
SELECT
    {{expression}}
FROM
    {{ model }}
WHERE
    DATA BETWEEN DATE("{{ var('start_timestamp') }}")
    AND DATE("{{ var('end_timestamp') }}")
GROUP BY
    {{expression}}
HAVING
    COUNT(*) > 1
{%- endtest %}