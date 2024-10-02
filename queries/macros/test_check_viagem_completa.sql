{% test check_viagem_completa(model) -%}
WITH
    data_versao_efetiva AS (
    SELECT
        *
    FROM
        rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva
    WHERE
        DATA >= "2024-04-01"
        AND DATA BETWEEN DATE("{{ var('start_timestamp') }}")
        AND DATE("{{ var('end_timestamp') }}")),
    viagem_completa AS (
    SELECT
        *
    FROM
        rj-smtr.projeto_subsidio_sppo.viagem_completa
    WHERE
        DATA >= "2024-04-01"
        AND DATA BETWEEN DATE("{{ var('start_timestamp') }}")
        AND DATE("{{ var('end_timestamp') }}")),
    feed_info AS (
    SELECT
        *
    FROM
        rj-smtr.gtfs.feed_info
    WHERE
        feed_version IN (
        SELECT
        feed_version
        FROM
        data_versao_efetiva) )
    SELECT
    DISTINCT DATA
    FROM
    viagem_completa
    LEFT JOIN
    data_versao_efetiva AS d
    USING
    (DATA)
    LEFT JOIN
    feed_info AS i
    ON
    (DATA BETWEEN i.feed_start_date
        AND i.feed_end_date
        OR (DATA >= i.feed_start_date
        AND i.feed_end_date IS NULL))
    WHERE
    i.feed_start_date != d.feed_start_date
    OR datetime_ultima_atualizacao < feed_update_datetime
{%- endtest %}