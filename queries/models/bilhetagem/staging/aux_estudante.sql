{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        partition_by={
            "field": "id_cliente",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000, "interval": 10000},
        },
    )
}}


{% set staging_estudante = ref("staging_estudante") %}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

-- busca quais partições serão atualizadas pelas capturas
{% if execute and is_incremental() %}
    {% set columns = (
        list_columns()
        | reject(
            "in",
            ["versao", "datetime_ultima_atualizacao", "id_execucao_dbt"],
        )
        | list
    ) %}
    {% set sha_column %}
        sha256(
            concat(
                {% for c in columns %}
                    ifnull(cast({{ c }} as string), 'n/a')
                    {% if not loop.last %}, {% endif %}

                {% endfor %}
            )
        )
    {% endset %}
    {% set partitions_query %}
        with
            ids as (
                select distinct cast(cd_cliente as integer) as id
                from {{ staging_estudante }}
                where {{ incremental_filter }}
            ),
            grupos as (select distinct div(id, 10000) as group_id from ids),
            identifica_grupos_continuos as (
                select
                    group_id,
                    if(
                        lag(group_id) over (order by group_id) = group_id - 1, 0, 1
                    ) as id_continuidade
                from grupos
            ),
            grupos_continuos as (
                select
                    group_id, sum(id_continuidade) over (order by group_id) as id_continuidade
                from identifica_grupos_continuos
            )
        select
            distinct
            concat(
                "id_cliente_particao between ",
                min(group_id) over (partition by id_continuidade) * 10000,
                " and ",
                (max(group_id) over (partition by id_continuidade) + 1) * 10000 - 1
            )
        from grupos_continuos
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}


with
    estudante as (
        select distinct
            cast(cast(estd.cd_cliente as float64) as int64) as id_cliente,
            estd.numero_matricula,
            estd.nome,
            codigo_escola,
            esc.descricao as nome_escola,
            esc.id_rede_ensino,
            esc.id_cre as id_cre_escola,
            estd.data_inclusao,
            estd.timestamp_captura as datetime_captura
        from {{ staging_estudante }} estd
        left join {{ ref("staging_escola") }} esc using (codigo_escola)
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}

    ),
    dados_completos as (
        select *, 0 as priority
        from estudante
        {% if is_incremental() and partitions | length > 0 %}
            union all
            select
                id_cliente,
                numero_matricula,
                nome,
                codigo_escola,
                id_rede_ensino,
                id_cre_escola,
                data_inclusao,
                datetime_captura,
                1 as priority
            from {{ this }}
            where {{ partitions | join("\nor ") }}
        {% endif %}

    ),
    inicio_fim_validade as (select *,)
    dados_completos_deduplicados as (
        select *
        from dados_completos
        qualify
            row_number() over (
                partition by id_cliente, numero_matricula, codigo_escola
                order by timestamp_captura desc, priority
            )
            = 1
    )
select
    id_cliente,
    id_gratuidade,
    tipo_gratuidade,
    deficiencia_permanente,
    rede_ensino,
    data_inicio_validade,
    lead(data_inicio_validade) over (
        partition by id_cliente order by data_inicio_validade
    ) as data_fim_validade,
    timestamp_captura
from gratuidade_deduplicada
