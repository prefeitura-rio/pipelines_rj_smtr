{% test check_gps_capture(model, table_id, interval) -%}

WITH
    t AS (
    SELECT
        DATETIME(timestamp_array) AS timestamp_array
    FROM
        UNNEST( GENERATE_TIMESTAMP_ARRAY( TIMESTAMP("{{ var('date_range_start') }}"), TIMESTAMP("{{ var('date_range_end') }}"), INTERVAL {{ interval }} minute) ) AS timestamp_array
    WHERE
        timestamp_array < TIMESTAMP("{{ var('date_range_end') }}") ),
    logs_table AS (
    SELECT
        SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
        SAFE_CAST(sucesso AS BOOLEAN) sucesso,
        SAFE_CAST(erro AS STRING) erro,
        SAFE_CAST(DATA AS DATE) DATA
    FROM
        rj-smtr-staging.br_rj_riodejaneiro_onibus_gps_staging.{{ table_id }}_logs AS t ),
    logs AS (
    SELECT
        *,
        TIMESTAMP_TRUNC(timestamp_captura, minute) AS timestamp_array
    FROM
        logs_table
    WHERE
        DATA BETWEEN DATE(TIMESTAMP("{{ var('date_range_start') }}"))
        AND DATE(TIMESTAMP("{{ var('date_range_end') }}"))
        AND timestamp_captura BETWEEN "{{ var('date_range_start') }}"
        AND "{{ var('date_range_end') }}" )
    SELECT
        COALESCE(logs.timestamp_captura, t.timestamp_array) AS timestamp_captura,
        logs.erro
    FROM
        t
    LEFT JOIN
        logs
    ON
        logs.timestamp_array = t.timestamp_array
    WHERE
        logs.sucesso IS NOT TRUE

{%- endtest %}
