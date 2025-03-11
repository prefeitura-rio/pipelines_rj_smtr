{% macro get_license_date() %}
    select
        date(data_versao) as data
    from {{ ref('licenciamento_data_versao_efetiva') }}
    where data = "{{ var('run_date')}}"
{% endmacro %}
