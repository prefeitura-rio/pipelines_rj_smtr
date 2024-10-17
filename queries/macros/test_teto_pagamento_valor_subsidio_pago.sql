{% test teto_pagamento_valor_subsidio_pago(model, expression) -%}
WITH
{{ model.name }} AS (
    SELECT
        *
    FROM
        {{ model }}
    WHERE
        DATA BETWEEN DATE("{{ var('start_date') }}")
        AND DATE("{{ var('end_date') }}")),
subsidio_valor_km_tipo_viagem AS (
    SELECT
        data_inicio,
        data_fim,
        MAX(subsidio_km) AS subsidio_km_teto
    FROM
        `rj-smtr`.`dashboard_subsidio_sppo_staging`.`subsidio_valor_km_tipo_viagem`
        -- {{ ref('subsidio_valor_km_tipo_viagem') }}
    WHERE
        subsidio_km > 0
    GROUP BY
        1,
        2)
SELECT
    *
FROM
    {{ model.name }} AS s
LEFT JOIN
    subsidio_valor_km_tipo_viagem AS p
ON
    s.data BETWEEN p.data_inicio
    AND p.data_fim
WHERE
    NOT({{ expression }})
{%- endtest %}