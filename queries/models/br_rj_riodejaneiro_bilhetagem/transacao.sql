-- depends_on: {{ ref('operadoras_contato') }}
-- depends_on: {{ ref('servico_operadora') }}
-- depends_on: {{ ref('transacao_riocard') }}
{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
  DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  AND timestamp_captura BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")
{% endset %}

{% set transacao_staging = ref("staging_transacao") %}
{% set integracao_staging = ref("staging_integracao_transacao") %}
{% set transacao_ordem = ref("aux_transacao_id_ordem_pagamento") %}
{% if execute %}
    {% if is_incremental() %}
        {% set transacao_partitions_query %}
            WITH particoes_integracao AS (
                SELECT DISTINCT
                CONCAT("'", DATE(data_transacao), "'") AS data_transacao
                FROM
                    {{ integracao_staging }},
                    UNNEST([
                        data_transacao_t0,
                        data_transacao_t1,
                        data_transacao_t2,
                        data_transacao_t3,
                        data_transacao_t4
                    ]) AS data_transacao
                WHERE
                    {{ incremental_filter }}
            ),
            particoes_transacao AS (
                SELECT DISTINCT
                    CONCAT("'", DATE(data_transacao), "'") AS data_transacao
                FROM
                    {{ transacao_staging }}
                WHERE
                    {{ incremental_filter }}
            ),
            particoes_transacao_ordem AS (
                 SELECT
                    CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data_transacao
                FROM
                    `rj-smtr.{{ transacao_ordem.schema }}.INFORMATION_SCHEMA.PARTITIONS`
                WHERE
                    table_name = "{{ transacao_ordem.identifier }}"
                    AND partition_id != "__NULL__"
                    AND DATETIME(last_modified_time, "America/Sao_Paulo") BETWEEN DATETIME("{{var('date_range_start')}}") AND (DATETIME("{{var('date_range_end')}}"))
            )
            SELECT
                data_transacao
            FROM
                particoes_transacao
            WHERE
                data_transacao IS NOT NULL
            UNION DISTINCT
            SELECT
                data_transacao
            FROM
                particoes_integracao
            WHERE
                data_transacao IS NOT NULL
            UNION DISTINCT
            SELECT
                data_transacao
            FROM
                particoes_transacao_ordem
            WHERE
                data_transacao IS NOT NULL

        {% endset %}

        {% set transacao_partitions = run_query(transacao_partitions_query) %}

        {% set transacao_partition_list = transacao_partitions.columns[0].values() %}

    {% endif %}
{% endif %}

with
    transacao as (
        select *
        from {{ transacao_staging }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    tipo_transacao as (
        select chave as id_tipo_transacao, valor as tipo_transacao,
        from `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario`
        where id_tabela = "transacao" and coluna = "id_tipo_transacao"
    ),
    gratuidade as (
        select
            cast(id_cliente as string) as id_cliente,
            tipo_gratuidade,
            data_inicio_validade,
            data_fim_validade
        from {{ ref("gratuidade_aux") }}
    -- `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.gratuidade_aux`
    -- TODO: FILTRAR PARTIÇÕES DE FORMA EFICIENTE
    ),
    tipo_pagamento as (
        select chave as id_tipo_pagamento, valor as tipo_pagamento
        from `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario`
        where id_tabela = "transacao" and coluna = "id_tipo_pagamento"
    ),
    integracao as (
        select id_transacao, valor_rateio, datetime_processamento_integracao
        {# from {{ ref("integracao") }} #}
        from `rj-smtr.br_rj_riodejaneiro_bilhetagem.integracao`
        {% if is_incremental() %}
            where
                {% if transacao_partition_list | length > 0 %}
                    data in ({{ transacao_partition_list | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        {% endif %}
        qualify
            row_number() over (
                partition by id_transacao
                order by datetime_processamento_integracao desc
            )
            = 1
    ),
    transacao_ordem as (
        select *
        {# from {{ ref("aux_transacao_id_ordem_pagamento") }} #}
        from `rj-smtr.bilhetagem_staging.aux_transacao_id_ordem_pagamento`
        {% if is_incremental() %}
            where
                {% if transacao_partition_list | length > 0 %}
                    data in ({{ transacao_partition_list | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        {% endif %}
    ),
    new_data as (
        select
            extract(date from data_transacao) as data,
            extract(hour from data_transacao) as hora,
            data_transacao as datetime_transacao,
            data_processamento as datetime_processamento,
            t.timestamp_captura as datetime_captura,
            m.modo,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            t.cd_linha as id_servico_jae,
            -- s.servico,
            l.nr_linha as servico_jae,
            l.nm_linha as descricao_servico_jae,
            sentido,
            case
                when m.modo = "VLT"
                then substring(t.veiculo_id, 1, 3)
                when m.modo = "BRT"
                then null
                else t.veiculo_id
            end as id_veiculo,
            t.numero_serie_validador as id_validador,
            coalesce(t.id_cliente, t.pan_hash) as id_cliente,
            id as id_transacao,
            tp.tipo_pagamento,
            tt.tipo_transacao,
            g.tipo_gratuidade,
            tipo_integracao as id_tipo_integracao,
            null as id_integracao,
            latitude_trx as latitude,
            longitude_trx as longitude,
            st_geogpoint(longitude_trx, latitude_trx) as geo_point_transacao,
            null as stop_id,
            null as stop_lat,
            null as stop_lon,
            valor_transacao
        from transacao as t
        left join
            {# {{ source("cadastro", "modos") }} m #}
            `rj-smtr.cadastro.modos` m
            on t.id_tipo_modal = m.id_modo
            and m.fonte = "jae"
        left join
            {# {{ ref("operadoras") }} do #}
            `rj-smtr.cadastro.operadoras` do on t.cd_operadora = do.id_operadora_jae
        left join
            {# {{ ref("consorcios") }} dc #}
            `rj-smtr.cadastro.consorcios` dc on t.cd_consorcio = dc.id_consorcio_jae
        left join {{ ref("staging_linha") }} l on t.cd_linha = l.cd_linha
        -- LEFT JOIN
        -- {{ ref("servicos") }} AS s
        -- ON
        -- t.cd_linha = s.id_servico_jae
        left join tipo_transacao tt on tt.id_tipo_transacao = t.tipo_transacao
        left join tipo_pagamento tp on t.id_tipo_midia = tp.id_tipo_pagamento
        left join
            gratuidade g
            on t.tipo_transacao = "21"
            and t.id_cliente = g.id_cliente
            and t.data_transacao >= g.data_inicio_validade
            and (t.data_transacao < g.data_fim_validade or g.data_fim_validade is null)
        left join
            {{ ref("staging_linha_sem_ressarcimento") }} lsr
            on t.cd_linha = lsr.id_linha
        where lsr.id_linha is null and date(data_transacao) >= "2023-07-17"
    ),
    {% if is_incremental() %}
        transacao_atual as (
            select *
            from {{ this }}
            where
                {% if transacao_partition_list | length > 0 %}
                    data in ({{ transacao_partition_list | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        ),
    {% endif %}
    complete_partitions as (
        select
            data,
            hora,
            datetime_transacao,
            datetime_processamento,
            datetime_captura,
            modo,
            id_consorcio,
            consorcio,
            id_operadora,
            operadora,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,
            sentido,
            id_veiculo,
            id_validador,
            id_cliente,
            id_transacao,
            tipo_pagamento,
            tipo_transacao,
            tipo_gratuidade,
            id_tipo_integracao,
            id_integracao,
            latitude,
            longitude,
            geo_point_transacao,
            stop_id,
            stop_lat,
            stop_lon,
            valor_transacao,
            0 as priority
        from new_data

        {% if is_incremental() %}
            union all

            select
                data,
                hora,
                datetime_transacao,
                datetime_processamento,
                datetime_captura,
                modo,
                id_consorcio,
                consorcio,
                id_operadora,
                operadora,
                id_servico_jae,
                servico_jae,
                descricao_servico_jae,
                sentido,
                id_veiculo,
                id_validador,
                id_cliente,
                id_transacao,
                tipo_pagamento,
                tipo_transacao,
                tipo_gratuidade,
                id_tipo_integracao,
                id_integracao,
                latitude,
                longitude,
                geo_point_transacao,
                stop_id,
                stop_lat,
                stop_lon,
                valor_transacao,
                1 as priority
            from transacao_atual
        {% endif %}
    ),
    transacao_deduplicada as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_transacao
                        order by datetime_captura desc, priority
                    ) as rn
                from complete_partitions
            )
        where rn = 1
    ),
    transacao_final as (
        select
            t.data,
            t.hora,
            t.datetime_transacao,
            t.datetime_processamento,
            t.datetime_captura,
            t.modo,
            t.id_consorcio,
            t.consorcio,
            t.id_operadora,
            t.operadora,
            t.id_servico_jae,
            t.servico_jae,
            t.descricao_servico_jae,
            t.sentido,
            t.id_veiculo,
            t.id_validador,
            t.id_cliente,
            sha256(t.id_cliente) as hash_cliente,
            t.id_transacao,
            t.tipo_pagamento,
            t.tipo_transacao,
            case
                when t.tipo_transacao = "Integração" or i.id_transacao is not null
                then "Integração"
                when t.tipo_transacao in ("Débito", "Botoeira")
                then "Integral"
                else t.tipo_transacao
            end as tipo_transacao_smtr,
            t.tipo_gratuidade,
            t.id_tipo_integracao,
            t.id_integracao,
            t.latitude,
            t.longitude,
            t.geo_point_transacao,
            t.stop_id,
            t.stop_lat,
            t.stop_lon,
            t.valor_transacao,
            case
                when
                    i.id_transacao is not null
                    or o.id_transacao is not null
                    or date(t.datetime_processamento)
                    {# < (select max(data_ordem) from {{ ref("ordem_pagamento_dia") }}) #}
                    < (
                        select max(data_ordem)
                        from `rj-smtr.br_rj_riodejaneiro_bilhetagem.ordem_pagamento_dia`
                    )
                then coalesce(i.valor_rateio, t.valor_transacao) * 0.96
            end as valor_pagamento,
            o.data_ordem,
            o.id_ordem_pagamento_servico_operador_dia,
            o.id_ordem_pagamento_consorcio_operador_dia,
            o.id_ordem_pagamento_consorcio_dia,
            o.id_ordem_pagamento
        from transacao_deduplicada t
        left join integracao i using (id_transacao)
        left join transacao_ordem o using (id_transacao)
    )
    {% set columns = (
        list_columns()
        | reject("in", ["versao", "datetime_ultima_atualizacao"])
        | list
    ) %}
select
    f.*,
    '{{ var("version") }}' as versao,
    {% if is_incremental() %}
        case
            when
                a.id_transacao is null
                or sha256(
                    concat(
                        {% for c in columns %}
                            {% if c == "geo_point_transacao" %}
                                ifnull(st_astext(f.geo_point_transacao), 'n/a')
                            {% elif c == "hash_cliente" %}
                                ifnull(to_base64(f.hash_cliente), 'n/a')
                            {% else %}ifnull(cast(f.{{ c }} as string), 'n/a')
                            {% endif %}

                            {% if not loop.last %}, {% endif %}

                        {% endfor %}
                    )
                ) != sha256(
                    concat(
                        {% for c in columns %}
                            {% if c == "geo_point_transacao" %}
                                ifnull(st_astext(a.geo_point_transacao), 'n/a')
                            {% elif c == "hash_cliente" %}
                                ifnull(to_base64(f.hash_cliente), 'n/a')
                            {% else %}ifnull(cast(a.{{ c }} as string), 'n/a')
                            {% endif %}

                            {% if not loop.last %}, {% endif %}

                        {% endfor %}
                    )
                )
            then current_datetime("America/Sao_Paulo")
            else a.datetime_ultima_atualizacao
        end
    {% else %} current_datetime("America/Sao_Paulo")
    {% endif %} as datetime_ultima_atualizacao
from transacao_final f
{% if is_incremental() %} left join transacao_atual a using (id_transacao) {% endif %}
