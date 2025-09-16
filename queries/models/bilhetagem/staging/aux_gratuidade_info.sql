{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_unico",
        partition_by={
            "field": "id_cliente",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000, "interval": 10000},
        },
    )
}}

{% set staging_estudante = ref("staging_estudante") %}
{% set staging_laudo_pcd = ref("staging_laudo_pcd") %}
{% set staging_gratuidade = ref("staging_gratuidade") %}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

-- busca quais partições serão atualizadas pelas capturas
{% if execute and is_incremental() %}
    {% set partitions_query %}
        with
            ids as (
                select distinct cast(cd_cliente as integer) as id
                from {{ staging_estudante }}
                where cd_cliente is not null and ({{ incremental_filter }})

                union distinct

                select distinct cast(cd_cliente as integer) as id
                from {{ staging_gratuidade }}
                where {{ incremental_filter }}

                union distinct

                select distinct cast(cd_cliente as integer) as id
                from {{ staging_laudo_pcd }}
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
                "id_cliente between ",
                min(group_id) over (partition by id_continuidade) * 10000,
                " and ",
                (max(group_id) over (partition by id_continuidade) + 1) * 10000 - 1
            )
        from grupos_continuos
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

with
    gratuidade as (
        select
            * except (datetime_inicio_validade, datetime_fim_validade),
            datetime_inicio_validade as datetime_inicio_validade_gratuidade,
            datetime_fim_validade as datetime_fim_validade_gratuidade
        from {{ ref("aux_gratuidade") }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %} {{ partitions | join("\nor ") }}
                {% else %} false
                {% endif %}
        {% endif %}
    ),
    estudante as (
        select
            * except (datetime_inicio_validade, datetime_fim_validade),
            datetime_inicio_validade as datetime_inicio_validade_estudante,
            datetime_fim_validade as datetime_fim_validade_estudante
        from {{ ref("aux_estudante") }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %} {{ partitions | join("\nor ") }}
                {% else %} false
                {% endif %}
        {% endif %}
    ),
    laudo_pcd as (
        select
            * except (datetime_inicio_validade, datetime_fim_validade),
            datetime_inicio_validade as datetime_inicio_validade_saude,
            datetime_fim_validade as datetime_fim_validade_saude
        from {{ ref("aux_laudo_pcd") }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %} {{ partitions | join("\nor ") }}
                {% else %} false
                {% endif %}
        {% endif %}
    ),
    gratuidade_estudante as (
        select
            g.id_cliente,
            g.id_gratuidade,
            g.id_cliente_gratuidade,
            g.tipo_gratuidade,
            e.codigo_escola,
            e.nome_escola,
            e.rede_ensino,
            e.id_cre_escola,
            case
                when
                    g.datetime_inicio_validade_gratuidade
                    > e.datetime_inicio_validade_estudante
                then g.datetime_inicio_validade_gratuidade
                else e.datetime_inicio_validade_estudante
            end as datetime_inicio_validade_gratuidade
        from gratuidade g
        left join estudante e using (id_cliente)
        where g.tipo_gratuidade = 'Estudante'
    ),
    gratuidade_estudante_fim_validade as (
        select
            *,
            lead(datetime_inicio_validade_gratuidade) over (
                partition by id_cliente order by datetime_inicio_validade_gratuidade
            ) as datetime_fim_validade_gratuidade
        from gratuidade_estudante
        qualify
            datetime_inicio_validade_gratuidade != datetime_fim_validade_gratuidade
            or datetime_fim_validade_gratuidade is null
    ),
    gratuidade_saude as (
        select
            g.id_cliente,
            g.id_gratuidade,
            g.id_cliente_gratuidade,
            g.tipo_gratuidade,
            l.deficiencia_permanente,
            case
                when
                    g.datetime_inicio_validade_gratuidade
                    > l.datetime_inicio_validade_saude
                then g.datetime_inicio_validade_gratuidade
                else l.datetime_inicio_validade_saude
            end as datetime_inicio_validade_gratuidade
        from gratuidade g
        left join laudo_pcd l using (id_cliente)
        where g.tipo_gratuidade = 'PCD'
    ),
    gratuidade_saude_fim_validade as (
        select
            *,
            lead(datetime_inicio_validade_gratuidade) over (
                partition by id_cliente order by datetime_inicio_validade_gratuidade
            ) as datetime_fim_validade_gratuidade
        from gratuidade_saude
        qualify
            datetime_inicio_validade_gratuidade != datetime_fim_validade_gratuidade
            or datetime_fim_validade_gratuidade is null
    ),
    outras_gratuidades as (
        select
            id_cliente,
            id_gratuidade,
            id_cliente_gratuidade,
            tipo_gratuidade,
            datetime_inicio_validade_gratuidade,
            datetime_fim_validade_gratuidade
        from gratuidade
        where tipo_gratuidade not in ('Estudante', 'PCD')
    ),
    union_gratuidade as (
        select
            id_cliente,
            id_gratuidade,
            id_cliente_gratuidade,
            tipo_gratuidade,
            codigo_escola,
            nome_escola,
            rede_ensino,
            id_cre_escola,
            cast(null as bool) as deficiencia_permanente,
            datetime_inicio_validade_gratuidade as datetime_inicio_validade,
            datetime_fim_validade_gratuidade as datetime_fim_validade
        from gratuidade_estudante_fim_validade

        union all

        select
            id_cliente,
            id_gratuidade,
            id_cliente_gratuidade,
            tipo_gratuidade,
            cast(null as string) as codigo_escola,
            cast(null as string) as nome_escola,
            cast(null as string) as rede_ensino,
            cast(null as string) as id_cre_escola,
            deficiencia_permanente,
            datetime_inicio_validade_gratuidade as datetime_inicio_validade,
            datetime_fim_validade_gratuidade as datetime_fim_validade
        from gratuidade_saude_fim_validade

        union all

        select
            id_cliente,
            id_gratuidade,
            id_cliente_gratuidade,
            tipo_gratuidade,
            cast(null as string) as codigo_escola,
            cast(null as string) as nome_escola,
            cast(null as string) as rede_ensino,
            cast(null as string) as id_cre_escola,
            cast(null as bool) as deficiencia_permanente,
            datetime_inicio_validade_gratuidade as datetime_inicio_validade,
            datetime_fim_validade_gratuidade as datetime_fim_validade
        from outras_gratuidades
    ),
    gratuidade_filtrada as (
        select
            concat(id_cliente_gratuidade, '-', datetime_inicio_validade) as id_unico, *
        from union_gratuidade
        qualify
            row_number() over (
                partition by id_cliente, datetime_inicio_validade
                order by tipo_gratuidade
            )
            = 1
    ),
    nova_validade as (
        select
            * except (datetime_fim_validade),
            lead(datetime_inicio_validade) over (
                partition by id_cliente order by datetime_inicio_validade
            ) as datetime_fim_validade
        from gratuidade_filtrada
    )
select *
from nova_validade
