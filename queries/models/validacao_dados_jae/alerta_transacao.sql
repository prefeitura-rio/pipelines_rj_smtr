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
    transacoes as (
        select
        {% if is_incremental() %}
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

        {% else %} * from {{ ref("transacao") }}
        {% endif %}
    ),
    transacao_cliente_cartao as (
        select
            *,
            if
            (
                cadastro_cliente = 'Cadastrado', to_base64(hash_cliente), hash_cartao
            ) as cliente_cartao,
        from transacoes
    ),
    transacao_intervalo as (
        select
            data,
            id_operadora,
            documento_operadora,
            id_validador,
            ov.placa,
            cliente_cartao,
            min(datetime_transacao) over (win) as datetime_inicio_intervalo,
            datetime_transacao as datetime_fim_intervalo,
            count(1) over (win) as quantidade_transacao,
            array_agg(id_transacao) over (win) as ids_transacao,
            latitude as latitude_ultima_transacao,
            longitude as longitude_ultima_transacao
        from transacao_cliente_cartao t
        left join
            `rj-smtr-dev.botelho__cadastro.operador_van_v2` ov
            on t.documento_operadora = ov.cpf
        where modo = 'Van'
        qualify quantidade_transacao >= 5
        window
            win as (
                partition by cliente_cartao
                order by unix_seconds(timestamp(datetime_transacao))
                range between 600 preceding and current row
            )

    ),
    particao_completa as (
        select *, 0 as priority
        from transacao_intervalo

        {% if is_incremental() %}
            union all

            select * except (versao), 1 as priority
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
select *, "{{ var('version') }}" as versao
from dados_deduplicados
