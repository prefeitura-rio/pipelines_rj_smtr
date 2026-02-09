{{
    config(
        partition_by={"field": "data", "data_type": "date", "granularity": "month"},
    )
}}

{% set incremental_filter %}
  {% if is_incremental() %}
    data BETWEEN DATE_TRUNC(DATE("{{ var('start_date') }}"), MONTH)
    AND LAST_DAY(DATE("{{ var('end_date') }}"), MONTH)
    AND data < DATE_TRUNC(CURRENT_DATE("America/Sao_Paulo"), MONTH)
    AND data >= DATE_TRUNC(DATE("{{ var('DATA_SUBSIDIO_V15_INICIO') }}"), MONTH)
  {% else %}
    data < DATE_TRUNC(CURRENT_DATE("America/Sao_Paulo"), MONTH)
  {% endif %}
{% endset %}

with
    licenciamento as (
        select data, id_veiculo, ano_fabricacao
        from {{ ref("veiculo_dia") }}
        where
            {{ incremental_filter }}
            and data >= date_trunc(date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}"), month)
        union distinct
        select data, id_veiculo, ano_fabricacao
        from {{ ref("sppo_licenciamento") }}
        -- rj-smtr.veiculo.sppo_licenciamento
        where
            {{ incremental_filter }}
            and data < date_trunc(date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}"), month)
    ),

    -- 1. Seleciona a última data disponível de cada mês
    datas as (
        select
            extract(month from data) as mes,
            extract(year from data) as ano,
            max(data) as data
        from licenciamento
        group by 1, 2
    ),

    -- 2. Verifica frota operante
    frota_operante as (
        select distinct
            id_veiculo, extract(month from data) as mes, extract(year from data) as ano
        from {{ ref("viagem_completa") }}
        -- rj-smtr.projeto_subsidio_sppo.viagem_completa
        where {{ incremental_filter }}
    ),

    -- 3. Calcula a idade de todos os veículos para a data de referência
    idade_frota as (
        select data, extract(year from data) - cast(ano_fabricacao as int64) as idade
        from datas
        left join licenciamento using (data)
        left join frota_operante as f using (id_veiculo, mes, ano)
        where f.id_veiculo is not null
    )

-- 4. Calcula a idade média
select
    data,
    extract(year from data) as ano,
    extract(month from data) as mes,
    "Ônibus" as modo,
    round(avg(idade), 2) as idade_media_veiculo_mes,
    current_date("America/Sao_Paulo") as data_ultima_atualizacao,
    '{{ var("version") }}' as versao
from idade_frota
group by 1, 2, 3
