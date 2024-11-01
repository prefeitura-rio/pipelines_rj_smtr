{% test completude(model, table_id) -%}
WITH
    time_array AS (
    SELECT
        *
    FROM
        UNNEST(GENERATE_DATE_ARRAY(DATE("{{ var('date_range_start') }}"), DATE("{{ var('date_range_end') }}"))) AS DATA ),
    {{ model.name }} AS (
    SELECT
        DATA,
        COUNT(*) AS q_registros
    FROM
        {{ model }}
    WHERE
        DATA BETWEEN DATE("{{ var('date_range_start') }}")
        AND DATE("{{ var('date_range_end') }}")
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