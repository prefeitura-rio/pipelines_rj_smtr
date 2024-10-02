{% test accepted_values_valor_penalidade(model) -%}
WITH
    all_values AS (
    SELECT
        DISTINCT valor_penalidade AS value_field,
        COUNT(*) AS n_records
    FROM
        {{ model }}
    WHERE
        DATA BETWEEN DATE("{{ var('start_timestamp') }}")
        AND DATE("{{ var('end_timestamp') }}")
    GROUP BY
        valor_penalidade )
    SELECT
        *
    FROM
        all_values
    WHERE
        value_field NOT IN (
            SELECT
                valor
            FROM
                `rj-smtr`.`dashboard_subsidio_sppo`.`valor_tipo_penalidade` )
{%- endtest %}