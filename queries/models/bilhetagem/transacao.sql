{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}


{% set transacao_ordem = ref("aux_transacao_id_ordem_pagamento") %}
{% set aux_transacao_particao = ref("aux_transacao_particao") %}

{% set incremental_filter %}
    generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end'))
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

{% if execute %}
    {% if is_incremental() %}
        {% set transacao_partitions_query %}
            select distinct
                particao
            from
                (
                    select
                        array_concat_agg(particoes) as particoes
                    from
                        {{ aux_transacao_particao }}
                ),
                unnest(particoes) as particao

            union distinct

            select
                concat("'", parse_date("%Y%m%d", partition_id), "'") as particao
            from
                `rj-smtr.{{ transacao_ordem.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ transacao_ordem.identifier }}"
                and partition_id != "__NULL__"
                and datetime(last_modified_time, "America/Sao_Paulo") between datetime("{{var('date_range_start')}}") and (datetime("{{var('date_range_end')}}"))

        {% endset %}

        {% set transacao_partitions = run_query(transacao_partitions_query).columns[0].values() %}
    {% endif %}
{% endif %}

with transacao_staging as (
    select
        *
    from {{ ref('staging_transacao') }}
    {% if is_incremental() %}
        where {{ incremental_filter }}
    {% endif %}
),
tipo_transacao as (
        select chave as id_tipo_transacao, valor as tipo_transacao,
        from {{ ref('dicionario_bilhetagem') }}
        {# from `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario` #}
        where id_tabela = "transacao" and coluna = "id_tipo_transacao"
),
gratuidade as (
    select
        cast(id_cliente as string) as id_cliente,
        tipo_gratuidade,
        data_inicio_validade,
        data_fim_validade
    from {{ ref("aux_gratuidade") }}
),
tipo_pagamento as (
    select chave as id_tipo_pagamento, valor as tipo_pagamento
    from {{ ref('dicionario_bilhetagem') }}
    {# from `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario` #}
    where id_tabela = "transacao" and coluna = "id_tipo_pagamento"
),
integracao as (
    select id_transacao, valor_rateio, datetime_processamento_integracao
    from {{ ref("integracao") }}
    {# from `rj-smtr.br_rj_riodejaneiro_bilhetagem.integracao` #}
    {% if is_incremental() %}
        where
            {% if transacao_partitions | length > 0 %}
                data in ({{ transacao_partitions | join(", ") }})
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
    from {{ ref("aux_transacao_id_ordem_pagamento") }}
    {% if is_incremental() %}
        where
            {% if transacao_partitions | length > 0 %}
                data in ({{ transacao_partitions | join(", ") }})
            {% else %} data = "2000-01-01"
            {% endif %}
    {% endif %}
),
transacao_nova as (
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
        t.cd_operadora as id_operadora_jae,
        do.operadora,
        t.cd_linha as id_servico_jae,
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
        t.id_cliente as id_cliente,
        t.pan_hash as hash_cartao,
        id as id_transacao,
        tp.tipo_pagamento,
        tt.tipo_transacao,
        g.tipo_gratuidade,
        g.deficiencia_permanente,
        g.rede_ensino,
        latitude_trx as latitude,
        longitude_trx as longitude,
        st_geogpoint(longitude_trx, latitude_trx) as geo_point_transacao,
        valor_transacao
    from transacao as t
    left join
        {{ source("cadastro", "modos") }} m
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
    left join {{ ref("staging_linha") }} l on t.cd_linha = l.cd_linha
    left join tipo_transacao tt on tt.id_tipo_transacao = t.tipo_transacao
    left join tipo_pagamento tp on t.id_tipo_midia = tp.id_tipo_pagamento
    left join
        gratuidade g
        on t.id_cliente = g.id_cliente
        and t.data_transacao >= g.data_inicio_validade
        and (t.data_transacao < g.data_fim_validade or g.data_fim_validade is null)
    left join
        {{ ref("staging_linha_sem_ressarcimento") }} lsr
        on t.cd_linha = lsr.id_linha
    where lsr.id_linha is null and date(data_transacao) >= "2023-07-17"
)