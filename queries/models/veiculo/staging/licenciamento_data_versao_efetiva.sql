{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    licenciamento as (
        select distinct date(data) as data_licenciamento
        from {{ ref("licenciamento_stu_staging") }}
        {% if is_incremental() %}
            where
                date(data)
                between "{{ var('run_date')}}"
                and "{{ modules.datetime.date.fromisoformat(var('run_date')) + modules.datetime.timedelta(7) }}"
        {% endif %}
    ),
    periodo as (
        select data
        from
            unnest(
                generate_date_array('2022-03-21', current_date("America/Sao_Paulo"))
            ) as data
        {% if is_incremental() %}
            where data between "{{ var('run_date')}}" and "{{ var('run_date')}}"
        {% endif %}
    ),
    data_versao_calc as (
        select
            periodo.data,
            (
                select
                    case
                        /* Versão fixa do STU em 2024-03-25 para mar/Q1 devido à falha de
             atualização na fonte da dados (SIURB) */
                        when
                            date(periodo.data) >= "2024-03-01"
                            and date(periodo.data) < "2024-03-16"
                        then date("2024-03-25")
                        /* Versão fixa do STU em 2024-04-09 para mar/Q2 devido à falha de
             atualização na fonte da dados (SIURB) */
                        when
                            date(periodo.data) >= "2024-03-16"
                            and date(periodo.data) < "2024-04-01"
                        then date("2024-04-09")
                        else
                            (
                                select min(date(data))
                                from {{ ref("licenciamento_stu_staging") }}
                                where
                                    date(data)
                                    >= date_add(date(periodo.data), interval 5 day)
                                    /* Admite apenas versões do STU igual ou após 2024-04-09 a
                         partir de abril/24 devido à falha de atualização na fonte
                         de dados (SIURB) */
                                    and (
                                        date(periodo.data) < "2024-04-01"
                                        or date(data) >= '2024-04-09'
                                    )
                            )
                    end
            ) as data_versao
        from periodo
    )
select *
from data_versao_calc
where data_versao is not null
