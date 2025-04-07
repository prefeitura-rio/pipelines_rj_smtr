{% macro get_violation_date() %}
    select
        case
            when date("{{ var('run_date') }}") between "2024-08-16" and "2024-10-15"
            then date("2024-10-22")
            else
                (
                    select min(date(data))
                    {# from {{ ref("infracao_staging") }} #}
                    from `rj-smtr.veiculo_staging.infracao`
                    where
                        date(data)
                        >= date_add(date("{{ var('run_date') }}"), interval 7 day)
                )
        END
{% endmacro %}
