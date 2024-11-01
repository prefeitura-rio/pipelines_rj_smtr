{% test accepted_values_in_table(model, column_name, ref_table, ref_column) %}
WITH
    all_values AS (
    SELECT
        DISTINCT {{ column_name }} AS value_field,
        COUNT(*) AS n_records
    FROM
        {{ model }}
    WHERE
        DATA BETWEEN DATE("{{ var('date_range_start') }}")
        AND DATE("{{ var('date_range_end') }}")
    GROUP BY
        {{ column_name }}
    )
    SELECT
        *
    FROM
        all_values
    WHERE
        value_field NOT IN (
            SELECT
                {{ ref_column }}
            FROM
                {{ ref(ref_table) }}
        )
{%- endtest %}