-- depends_on: {{ ref('aux_sppo_licenciamento_vistoria_atualizada') }}
{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

{% if execute %}
  {% set licenciamento_date = run_query(get_license_date()).columns[0].values()[0] %}
{% endif %}

with
    -- Tabela de licenciamento
    stu as (
        select
            * except(data),
            date(data) AS data
        from
            {{ ref("sppo_licenciamento_stu_staging") }} as t
        where
            date(data) = date("{{ licenciamento_date }}")
            and tipo_veiculo not like "%ROD%"
    ),
    stu_rn AS (
        select
            * except (timestamp_captura),
            EXTRACT(YEAR FROM data_ultima_vistoria) AS ano_ultima_vistoria,
            ROW_NUMBER() OVER (PARTITION BY data, id_veiculo) rn
        from
            stu
    ),
    stu_ano_ultima_vistoria AS (
        -- Temporariamente considerando os dados de vistoria enviados pela TR/SUBTT/CGLF
        {% if var("run_date") >= "2024-03-01" %}
        SELECT
            s.* EXCEPT(ano_ultima_vistoria),
            CASE
                WHEN c.ano_ultima_vistoria > s.ano_ultima_vistoria THEN c.ano_ultima_vistoria
                ELSE COALESCE(s.ano_ultima_vistoria, c.ano_ultima_vistoria)
            END AS ano_ultima_vistoria_atualizado,
        FROM
            stu_rn AS s
        LEFT JOIN
            (
                SELECT
                    id_veiculo,
                    placa,
                    ano_ultima_vistoria
                FROM
                    {{ ref("aux_sppo_licenciamento_vistoria_atualizada") }}
            ) AS c
        USING
            (id_veiculo, placa)
        {% else %}
        SELECT
            s.* EXCEPT(ano_ultima_vistoria),
            s.ano_ultima_vistoria AS ano_ultima_vistoria_atualizado,
        FROM
            stu_rn AS s
        {% endif %}
    )
select
  * except(rn),
from
  stu_ano_ultima_vistoria
where
  rn = 1

