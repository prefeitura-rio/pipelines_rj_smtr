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
    estudante as (
        select distinct
            cast(cast(estd.cd_cliente as float64) as int64) as id_cliente,
            estd.numero_matricula,
            estd.nome,
            codigo_escola,
            esc.nome as nome_escola,
            esc.rede_ensino,
            esc.id_cre as id_cre_escola,
            estd.data_inclusao as datetime_inclusao,
            estd.timestamp_captura as datetime_captura
        from {{ staging_estudante }} estd
        left join
            {{ ref("aux_escola_rede_ensino_atualizado") }} esc using (codigo_escola)
        where
            cd_cliente is not null
            {% if is_incremental() %} and ({{ incremental_filter }}) {% endif %}

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
                nome_escola,
                rede_ensino,
                id_cre_escola,
                datetime_inclusao,
                datetime_captura,
                1 as priority
            from {{ this }}
            where {{ partitions | join("\nor ") }}
        {% endif %}

    ),
    dados_completos_deduplicados as (
        select * except (priority)
        from dados_completos
        qualify
            row_number() over (
                partition by id_cliente, datetime_inclusao
                order by datetime_captura desc, priority
            )
            = 1
    ),
    lag_datas as (
        select
            *,
            lag(datetime_inclusao) over (
                partition by id_cliente order by datetime_inclusao
            ) as datetime_inclusao_anterior,
            lag(datetime_captura) over (
                partition by id_cliente order by datetime_inclusao
            ) as datetime_captura_anterior
        from dados_completos_deduplicados
    ),
    inicio_validade as (
        select
            * except (datetime_inclusao_anterior, datetime_captura_anterior),
            case
                when
                    datetime_inclusao_anterior is null
                    or datetime_inclusao_anterior != datetime_inclusao
                then datetime_inclusao
                else datetime_captura_anterior
            end as datetime_inicio_validade
        from lag_datas
    ),
    fim_validade as (
        select
            *,
            lead(datetime_inicio_validade) over (
                partition by id_cliente order by datetime_inicio_validade
            ) as datetime_fim_validade
        from inicio_validade
    )
select
    concat(id_cliente, '-', datetime_inclusao) as id_unico,
    id_cliente,
    row_number() over (
        partition by id_cliente order by datetime_inicio_validade
    ) as sequencia_cliente_estudante,
    numero_matricula,
    nome,
    codigo_escola,
    nome_escola,
    rede_ensino,
    id_cre_escola,
    datetime_inicio_validade,
    datetime_fim_validade,
    datetime_inclusao,
    datetime_captura
from fim_validade
