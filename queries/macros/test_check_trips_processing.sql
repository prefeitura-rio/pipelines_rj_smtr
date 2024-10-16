{% test check_trips_processing(model) -%}
SELECT
    s.data,
    s.tipo_dia,
    s.subtipo_dia,
    s.tipo_os,
    s.feed_version,
    s.feed_start_date AS feed_start_date_invalido,
    i.feed_start_date AS feed_start_date_valido,
FROM (
    SELECT
        *
    FROM
        rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva
    WHERE
        DATA >= "2024-04-01" -- DATA_SUBSIDIO_V6_INICIO (Feature trajetos alternativos)
        AND DATA BETWEEN DATE("{{ var('start_timestamp') }}")
            AND DATE("{{ var('end_timestamp') }}")
    ) AS s
LEFT JOIN
    -- rj-smtr.gtfs.feed_info AS i
    {{ ref('feed_info') }} AS i
ON
(DATA BETWEEN i.feed_start_date
    AND i.feed_end_date
    OR (DATA >= i.feed_start_date
    AND i.feed_end_date IS NULL))
WHERE
    i.feed_start_date != s.feed_start_date
{%- endtest %}