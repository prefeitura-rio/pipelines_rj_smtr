{% test completude(model) -%}
WITH
    time_array AS (
    SELECT
        *
    FROM
        UNNEST(GENERATE_DATE_ARRAY(DATE("{{ var('start_timestamp') }}"), DATE("{{ var('end_timestamp') }}"))) AS DATA ),
    {{ model.name }} AS (
    SELECT
        DATA,
        COUNT(*) AS q_registros
    FROM
        {{ model }}
    WHERE
        DATA BETWEEN DATE("{{ var('start_timestamp') }}")
        AND DATE("{{ var('end_timestamp') }}")
    GROUP BY
        1 )
SELECT
    DATA,
    q_registros
FROM
    time_array
LEFT JOIN
    {{ model.name }}
USING
    (DATA)
WHERE
    q_registros IS NULL
    OR q_registros = 0
{%- endtest %}