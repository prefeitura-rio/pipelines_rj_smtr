{% macro get_license_date() %}
    select
        case
            /* Versão fixa do STU em 2024-03-25 para mar/Q1 devido à falha de
             atualização na fonte da dados (SIURB) */
            when
                date("{{ var('run_date') }}") >= "2024-03-01"
                and date("{{ var('run_date') }}") < "2024-03-16"
            then date("2024-03-25")
            /* Versão fixa do STU em 2024-04-09 para mar/Q2 devido à falha de
             atualização na fonte da dados (SIURB) */
            when
                date("{{ var('run_date') }}") >= "2024-03-16"
                and date("{{ var('run_date') }}") < "2024-04-01"
            then date("2024-04-09")
            else
                (
                    select min(date(data))
                    from {{ ref("licenciamento_stu_staging") }}
                    where
                        date(data)
                        >= date_add(date("{{ var('run_date') }}"), interval 5 day)
                        /* Admite apenas versões do STU igual ou após 2024-04-09 a
                         partir de abril/24 devido à falha de atualização na fonte
                         de dados (SIURB) */
                        and (
                            date("{{ var('run_date') }}") < "2024-04-01"
                            or date(data) >= '2024-04-09'
                        )
                )
        end
{% endmacro %}
