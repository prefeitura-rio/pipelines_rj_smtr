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

-- TODO: Usar variável de run_date_hour para otimizar o numero de partições lidas em
-- staging
{% set incremental_filter %}
  DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  AND timestamp_captura BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")
{% endset %}

{% set transacao_staging = ref("staging_transacao") %}
{% set integracao_staging = ref("staging_integracao_transacao") %}
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
        )
        SELECT
          COALESCE(t.data_transacao, i.data_transacao) AS data_transacao
        FROM
          particoes_transacao t
        FULL OUTER JOIN
          particoes_integracao i
        USING(data_transacao)
        WHERE
          COALESCE(t.data_transacao, i.data_transacao) IS NOT NULL
        {% endset %}

        {% set transacao_partitions = run_query(transacao_partitions_query) %}

        {% set transacao_partition_list = transacao_partitions.columns[0].values() %}
    {% endif %}
{% endif %}

with
    transacao as (
        select *
        from {{ transacao_staging }}
        -- `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.transacao`
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
        from {{ ref("integracao") }}
        -- `rj-smtr.br_rj_riodejaneiro_bilhetagem.integracao`
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
            {{ source("cadastro", "modos") }} m
            -- `rj-smtr.cadastro.modos` m
            on t.id_tipo_modal = m.id_modo
            and m.fonte = "jae"
        left join
            {{ ref("operadoras") }} do
            -- `rj-smtr.cadastro.operadoras` do
            on t.cd_operadora = do.id_operadora_jae
        left join
            {{ ref("consorcios") }} dc
            -- `rj-smtr.cadastro.consorcios` dc
            on t.cd_consorcio = dc.id_consorcio_jae
        left join
            {{ ref("staging_linha") }} l
            -- `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.linha` l
            on t.cd_linha = l.cd_linha
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
            -- `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.linha_sem_ressarcimento`
            -- lsr
            on t.cd_linha = lsr.id_linha
        where lsr.id_linha is null and date(data_transacao) >= "2023-07-17"
    ),
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
            from {{ this }}
            where
                {% if transacao_partition_list | length > 0 %}
                    data in ({{ transacao_partition_list | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
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
    )
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
            or date(t.datetime_processamento)
            < (select max(data_ordem) from {{ ref("ordem_pagamento_dia") }})
        then coalesce(i.valor_rateio, t.valor_transacao) * 0.96
    end as valor_pagamento,
    '{{ var("version") }}' as versao
from transacao_deduplicada t
left join integracao i using (id_transacao)
