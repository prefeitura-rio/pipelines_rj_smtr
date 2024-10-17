{% test check_sppo_veiculo_dia(model) %}
WITH
    count_dist_status AS (
    SELECT
        DATA,
        COUNT(DISTINCT status) AS q_dist_status,
        NULL AS q_duplicated_status,
        NULL AS q_null_status
    FROM
        -- rj-smtr.veiculo.sppo_veiculo_dia
        {{ ref('sppo_veiculo_dia')}}
    WHERE
        DATA BETWEEN DATE("{{ var('start_date') }}")
        AND DATE("{{ var('end_date') }}")
    GROUP BY
        1
    HAVING
        COUNT(DISTINCT status) = 1 ),
    count_duplicated_status AS (
    SELECT
        DATA,
        id_veiculo,
        COUNT(*) AS q_status,
    FROM
       -- rj-smtr.veiculo.sppo_veiculo_dia
        {{ ref('sppo_veiculo_dia')}}
    WHERE
        DATA BETWEEN DATE("{{ var('start_date') }}")
        AND DATE("{{ var('end_date') }}")
    GROUP BY
        1,
        2
    HAVING
        COUNT(*) > 1 ),
    count_duplicated_status_agg AS (
    SELECT
        DATA,
        NULL AS q_dist_status,
        SUM(q_status) AS q_duplicated_status,
        NULL AS q_null_status
    FROM
        count_duplicated_status
    GROUP BY
        1),
    count_null_status AS (
    SELECT
        DATA,
        NULL AS q_dist_status,
        NULL AS q_duplicated_status,
        COUNT(*) AS q_null_status
    FROM
        -- rj-smtr.veiculo.sppo_veiculo_dia
        {{ ref('sppo_veiculo_dia')}}
    WHERE
        DATA BETWEEN DATE("{{ var('start_date') }}")
        AND DATE("{{ var('end_date') }}")
        AND status IS NULL
    GROUP BY
        1 )
SELECT
    *
FROM
    count_dist_status

UNION ALL

SELECT
    *
FROM
    count_duplicated_status_agg

UNION ALL

SELECT
    *
FROM
    count_null_status

{% endtest %}