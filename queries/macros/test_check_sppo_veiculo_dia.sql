{% test check_sppo_veiculo_dia(model) %}
    WITH
        count_dist_status AS (
        SELECT
            DATA,
            COUNT(DISTINCT status) AS q_dist_status,
        FROM
            -- rj-smtr.veiculo.sppo_veiculo_dia
            {{ ref('sppo_veiculo_dia')}}
        WHERE
            DATA BETWEEN DATE("{{ var('date_range_start') }}")
            AND DATE("{{ var('end_date') }}")
        GROUP BY
            1
        HAVING
            COUNT(DISTINCT status) = 1 )
    SELECT
        *
    FROM
        count_dist_status
{% endtest %}