{{ config(
        materialized="table"
    )
}}

WITH licenciamento AS (
  SELECT DISTINCT SAFE_CAST(data AS DATE) AS data_licenciamento
  FROM {{ ref('licenciamento_stu_staging') }}
),
periodo AS (
  SELECT DATE_ADD(DATE '2022-03-21', INTERVAL n DAY) AS data
  FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE '2026-01-01', DATE '2022-03-21', DAY))) AS n
),
data_versao_calc AS (
  SELECT
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
    ) AS data_versao
  FROM periodo
)
SELECT *
FROM data_versao_calc