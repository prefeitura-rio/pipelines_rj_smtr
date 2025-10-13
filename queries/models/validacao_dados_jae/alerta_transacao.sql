{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}
-- depends_on: {{ ref('staging_transacao') }}
-- depends_on: {{ ref('operadoras') }}
-- depends_on: {{ ref('modos') }}
-- depends_on: {{ ref('transacao') }}
with
    transacao_staging as (
        date(data_transacao) as data,
        id as id_transacao,
        data_transacao as datetime_transacao,
        m.modo,
        o.id_operadora,
        o.documento as documento_operadora,
        case
            when t.id_cliente is null or t.id_cliente = "733"
            then "Não Cadastrado"
            else "Cadastrado"
        end as cadastro_cliente,
        sha256(id_cliente) as hash_cliente,
        pan_hash as hash_cartao,
        latitude_trx as latitude,
        longitude_trx as longitude,
        numero_serie_validador as id_validador,
        from {{ ref("staging_transacao") }} t
        join {{ ref("operadoras") }} o on t.cd_operadora = o.id_operadora_jae
        left join
            {{ ref("modos") }} m on t.id_tipo_modal = m.id_modo and m.fonte = "jae"
        where
            (
                {{
                    generate_date_hour_partition_filter(
                        var("date_range_start"), var("date_range_end")
                    )
                }}
            )
            and data between date(
                datetime_sub(
                    datetime("{{ var('date_range_start') }}"), interval 5 minute
                )
            ) and date("{{ var('date_range_end') }}")
    ),
    transacao as (
        {% if is_incremental() %}select * from transacao_staging

        {% else %}
            select
                data,
                id_transacao,
                datetime_transacao,
                modo,
                id_operadora,
                documento_operadora,
                cadastro_cliente,
                hash_cliente,
                hash_cartao,
                latitude,
                longitude,
                id_validador
            from {{ ref("transacao") }}
            union all
            select
                data,
                id_transacao,
                datetime_transacao,
                modo,
                id_operadora,
                documento_operadora,
                cadastro_cliente,
                hash_cliente,
                hash_cartao,
                latitude,
                longitude,
                id_validador
            from transacao_staging
        {% endif %}
    ),
    transacao_deduplicada as (
        select *
        from transacao
        qualify row_number() over (partition by id_transacao order by data) = 1
    ),
    transacao_cliente_cartao as (
        select
            *,
            if
            (
                cadastro_cliente = 'Cadastrado', to_base64(hash_cliente), hash_cartao
            ) as cliente_cartao,
        from transacao_staging
    ),
    transacao_intervalo as (
        select
            data,
            id_operadora,
            regexp_replace(ov.numero_permissao, '[^0-9]', '') as permissao,
            documento_operadora,
            id_validador,
            ov.placa,
            cliente_cartao,
            min(datetime_transacao) over (win) as datetime_inicio_intervalo,
            datetime_transacao as datetime_fim_intervalo,
            count(1) over (win) as quantidade_transacao,
            array_agg(
                struct(
                    id_transacao as id_transacao,
                    latitude as latitude,
                    longitude as longitude,
                    datetime_transacao as datetime_transacao
                )
            ) over (win) as transacoes
        from transacao_cliente_cartao t
        left join
            {{ source("cadastro_staging", "operador_van_v2") }} ov
            on t.documento_operadora = ov.cpf
        where modo = 'Van'
        qualify quantidade_transacao >= 5
        window
            win as (
                partition by cliente_cartao, id_operadora, id_validador
                order by unix_seconds(timestamp(datetime_transacao))
                range between 600 preceding and current row
            )

    ),
    transacao_lat_long_filtrada as (
        select
            * except (transacoes),
            array(
                select t.id_transacao
                from unnest(transacoes) t
                order by t.datetime_transacao
            ) as ids_transacao,
            array(
                select struct(t.latitude as latitude, t.longitude as longitude)
                from unnest(transacoes) t
                where t.latitude != 0 and t.longitude != 0
                order by t.datetime_transacao desc
            )[safe_offset(0)] as ultima_lat_long_valida
        from transacao_intervalo

    ),
    transacao_lat_long as (
        select
            * except (ultima_lat_long_valida),
            case
                when ultima_lat_long_valida is not null
                then ultima_lat_long_valida.latitude
                else 0.0
            end as latitude_ultima_transacao,
            case
                when ultima_lat_long_valida is not null
                then ultima_lat_long_valida.longitude
                else 0.0
            end as longitude_ultima_transacao,
            "{{ var('version') }}" as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            '{{ invocation_id }}' as id_execucao_dbt
        from transacao_lat_long_filtrada
    ),
    particao_completa as (
        select *, 0 as priority
        from transacao_lat_long

        {% if is_incremental() %}
            union all

            select *, 1 as priority
            from {{ this }}
            where
                data between date(
                    datetime_sub(
                        datetime("{{ var('date_range_start') }}"), interval 5 minute
                    )
                ) and date("{{ var('date_range_end') }}")
        {% endif %}
    ),
    dados_deduplicados as (
        select * except (priority)
        from particao_completa
        qualify
            row_number() over (
                partition by id_operadora, cliente_cartao, datetime_inicio_intervalo
                order by priority
            )
            = 1
    )
select *
from dados_deduplicados
