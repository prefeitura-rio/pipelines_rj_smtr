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
    ({{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }})
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

-- busca quais partições serão atualizadas pelas capturas
{% if execute %}
    {% if is_incremental() %}
        {% set columns = (
            list_columns()
            | reject("in", ["versao", "datetime_ultima_atualizacao"])
            | list
        ) %}

        {% set transacao_partitions_query %}
            select distinct
                concat("'", particao, "'") as particao
            from
                (
                    select
                        array_concat_agg(particoes) as particoes
                    from
                        {{ aux_transacao_particao }}
                    where {{ incremental_filter }}
                ),
                unnest(particoes) as particao

            union distinct

            select
                concat("'", parse_date("%Y%m%d", partition_id), "'") as particao
            from
                `{{ transacao_ordem.database }}.{{ transacao_ordem.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ transacao_ordem.identifier }}"
                and partition_id != "__NULL__"
                and datetime(last_modified_time, "America/Sao_Paulo") between datetime("{{var('date_range_start')}}") and (datetime("{{var('date_range_end')}}"))

        {% endset %}

        {% set transacao_partitions = (
            run_query(transacao_partitions_query).columns[0].values()
        ) %}
    {% endif %}
{% endif %}

with
    transacao_staging as (
        select *
        from {{ ref("staging_transacao") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    tipo_transacao as (
        select chave as id_tipo_transacao, valor as tipo_transacao
        from {{ ref("dicionario_bilhetagem") }}
        {# from `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario` #}
        where id_tabela = "transacao" and coluna = "id_tipo_transacao"
    ),
    gratuidade as (
        select
            cast(id_cliente as string) as id_cliente,
            tipo_gratuidade,
            rede_ensino,
            deficiencia_permanente,
            data_inicio_validade,
            data_fim_validade
        from {{ ref("aux_gratuidade") }}
    ),
    tipo_pagamento as (
        select chave as id_tipo_pagamento, valor as tipo_pagamento
        from {{ ref("dicionario_bilhetagem") }}
        {# from `rj-smtr.bilhetagem.dicionario` #}
        where id_tabela = "transacao" and coluna = "id_tipo_pagamento"
    ),
    integracao as (
        select id_transacao, valor_rateio, datetime_processamento_integracao
        from {{ ref("integracao") }}
        {# from `rj-smtr.bilhetagem.integracao` #}
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
            id as id_transacao,
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
            sha256(t.id_cliente) as hash_cliente,
            t.pan_hash as hash_cartao,
            tp.tipo_pagamento as meio_pagamento_jae,
            p.nm_produto as produto_jae,
            tt.tipo_transacao as tipo_transacao_jae,
            latitude_trx as latitude,
            longitude_trx as longitude,
            st_geogpoint(longitude_trx, latitude_trx) as geo_point_transacao,
            valor_transacao
        from transacao_staging as t
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
        left join {{ ref("staging_produto") }} p on t.id_produto = p.cd_produto
        left join tipo_transacao tt on tt.id_tipo_transacao = t.tipo_transacao
        left join tipo_pagamento tp on t.id_tipo_midia = tp.id_tipo_pagamento
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
    particao_completa as (
        select
            data,
            hora,
            id_transacao,
            datetime_transacao,
            datetime_processamento,
            datetime_captura,
            modo,
            id_consorcio,
            consorcio,
            id_operadora,
            id_operadora_jae,
            operadora,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,
            sentido,
            id_veiculo,
            id_validador,
            id_cliente,
            hash_cliente,
            hash_cartao,
            meio_pagamento_jae,
            produto_jae,
            tipo_transacao_jae,
            latitude,
            longitude,
            geo_point_transacao,
            valor_transacao
        from transacao_nova

        {% if is_incremental() %}
            select
                data,
                hora,
                id_transacao,
                datetime_transacao,
                datetime_processamento,
                datetime_captura,
                modo,
                id_consorcio,
                consorcio,
                id_operadora,
                id_operadora_jae,
                operadora,
                id_servico_jae,
                servico_jae,
                descricao_servico_jae,
                sentido,
                id_veiculo,
                id_validador,
                id_cliente,
                hash_cliente,
                hash_cartao,
                meio_pagamento_jae,
                produto_jae,
                tipo_transacao_jae,
                latitude,
                longitude,
                geo_point_transacao,
                valor_transacao
            from transacao_atual
        {% endif %}
    ),
    -- Adiciona informações que são modificadas posteriormente pelo processo da Jaé
    transacao_info_posterior as (
        select
            t.*,
            case
                when
                    t.tipo_transacao_jae in ("Integração", "Integração EMV")
                    or i.id_transacao is not null
                then "Integração"
                when t.tipo_transacao_jae in ("Débito", "Débito EMV", "Botoeira")
                then "Integral"
                when t.tipo_transacao_jae = "Transferência EMV"
                then "Transferência"
                else t.tipo_transacao_jae
            end as tipo_transacao_atualizado,
            case
                when
                    i.id_transacao is not null
                    or o.id_transacao is not null
                    or date(t.datetime_processamento)
                    < (select max(data_ordem) from {{ ref("ordem_pagamento_dia") }})
                then coalesce(i.valor_rateio, t.valor_transacao) * 0.96
            end as valor_pagamento,
            o.data_ordem,
            o.id_ordem_pagamento_servico_operador_dia,
            o.id_ordem_pagamento_consorcio_operador_dia,
            o.id_ordem_pagamento_consorcio_dia,
            o.id_ordem_pagamento
        from particao_completa t
        left join integracao i using (id_transacao)
        left join transacao_ordem o using (id_transacao)
    ),
    -- Cria os níveis de classificação da transação
    transacao_nivel as (
        select
            t.*,
            case
                when t.id_cliente is null or t.id_cliente = "733"
                then "Não Cadastrado"
                else "Cadastrado"
            end as cadastro_cliente,
            case
                when t.produto_jae in ("Conta Jaé", "Conta digital")
                then "Carteira"
                when t.produto_jae = "Conta Jaé Gratuidade"
                then "Gratuidade"
                when t.produto_jae = "Conta Jaé VT"
                then "VT"
                when t.produto_jae = "Avulso"
                then "Cartão Avulso"
                when t.tipo_transacao_jae like "% EMV"
                then "Visa Internacional"
                when t.tipo_transacao_jae = "Botoeira"
                then "Dinheiro (Botoeira)"
            end as produto,
            case
                when
                    t.produto_jae = "Conta Jaé Gratuidade"
                    and t.tipo_transacao_jae != "Gratuidade"
                then concat(t.tipo_transacao_atualizado, " - Gratuidade")
                else t.tipo_transacao_atualizado
            end as tipo_transacao,
            case
                when
                    t.tipo_transacao_jae != "Gratuidade"
                    and t.produto_jae != "Conta Jaé Gratuidade"
                then "Pagante"
                when g.tipo_gratuidade = "Sênior"
                then "Idoso"
                when g.tipo_gratuidade = "Estudante" and g.rede_ensino is not null
                then concat("Estudante ", split(g.rede_ensino, " - ")[0])
                when g.tipo_gratuidade = "Estudante"
                then "Estudante Não Identificado"
                when g.tipo_gratuidade = "PCD" and g.deficiencia_permanente
                then "PCD"
                when g.tipo_gratuidade = "PCD" and not g.deficiencia_permanente
                then "DC"
                else "Não Identificado"
            end as tipo_usuario,
            case
                when t.meio_pagamento_jae like "Cartão%"
                then "Cartão"
                when t.tipo_transacao_jae = "Botoeira"
                then "Dinheiro"
                else t.meio_pagamento_jae
            end as meio_pagamento
        from transacao_info_posterior t
        left join
            gratuidade g
            on t.id_cliente = g.id_cliente
            and t.datetime_transacao >= g.data_inicio_validade
            and (
                t.datetime_transacao < g.data_fim_validade
                or g.data_fim_validade is null
            )
    ),
    transacao_colunas_ordenadas as (
        select
            data,
            hora,
            id_transacao,
            datetime_transacao,
            datetime_processamento,
            datetime_captura,
            modo,
            id_consorcio,
            consorcio,
            id_operadora,
            id_operadora_jae,
            operadora,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,
            sentido,
            id_veiculo,
            id_validador,
            id_cliente,
            hash_cliente,
            hash_cartao,
            cadastro_cliente,
            produto,
            produto_jae,
            tipo_transacao,
            tipo_transacao_jae,
            tipo_usuario,
            meio_pagamento,
            meio_pagamento_jae,
            latitude,
            longitude,
            geo_point_transacao,
            valor_transacao,
            valor_pagamento,
            data_ordem,
            id_ordem_pagamento_servico_operador_dia,
            id_ordem_pagamento_consorcio_operador_dia,
            id_ordem_pagamento_consorcio_dia,
            id_ordem_pagamento
        from transacao_nivel
    )

select
    t.*,
    '{{ var("version") }}' as versao,
    -- Atualiza datetime_ultima_atualizacao apenas se houver alteração do valor em
    -- algum campo
    {% if is_incremental() %}
        case
            when
                a.id_transacao is null
                or sha256(
                    concat(
                        {% for c in columns %}
                            {% if c == "geo_point_transacao" %}
                                ifnull(st_astext(t.geo_point_transacao), 'n/a')
                            {% elif c == "hash_cliente" %}
                                ifnull(to_base64(t.hash_cliente), 'n/a')
                            {% else %}ifnull(cast(t.{{ c }} as string), 'n/a')
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
                                ifnull(to_base64(a.hash_cliente), 'n/a')
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
from transacao_colunas_ordenadas t
{% if is_incremental() %} left join transacao_atual a using (id_transacao) {% endif %}
