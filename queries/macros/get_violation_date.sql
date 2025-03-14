{% macro get_violation_date() %}
    select min(safe_cast(data as date))
    from {{ ref("infracao_staging") }}
    where
        safe_cast(data as date)
        >= date_add(date("{{ var('run_date') }}"), interval 7 day)
{% endmacro %}
