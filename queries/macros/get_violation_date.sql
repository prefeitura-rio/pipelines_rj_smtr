{% macro get_violation_dates() %}
    -- juntar com get_license_datess
    select
        min(date(data_versao)) as min_data,
        coalesce(max(date(data_versao)), min(date(data_versao))) as max_data
    from {{ ref('infracao_data_versao_efetiva') }}
    where data between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
{% endmacro %}
