-- depends_on: {{ ref('matriz_integracao') }}
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set incremental_filter %}
    date(data) between date("{{var('date_range_start')}}") and date(
                            "{{var('date_range_end')}}"
    )
    and timestamp_captura
    between datetime("{{var('date_range_start')}}") and datetime(
        "{{var('date_range_end')}}"
                        )
{% endset %}

{% set integracao_staging = ref("staging_integracao_transacao") %}

{% if execute %}
    {% if is_incremental() %}
        {% set integracao_partitions_query %}

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

        {% endset %}

        {% set integracao_partitions = (
            run_query(integracao_partitions_query).columns[0].values()
        ) %}

    {% endif %}
{% endif %}

with
    integracao_transacao_deduplicada as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id order by timestamp_captura desc
                    ) as rn
                from {{ integracao_staging }}
                {# `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.integracao_transacao` #}
                {% if is_incremental() -%} where {{ incremental_filter }} {%- endif %}
            )
        where rn = 1
    ),
    integracao_melt as (
        select
            extract(date from im.data_transacao) as data,
            extract(hour from im.data_transacao) as hora,
            i.data_inclusao as datetime_inclusao,
            i.data_processamento as datetime_processamento_integracao,
            i.timestamp_captura as datetime_captura,
            i.id as id_integracao,
            im.sequencia_integracao,
            im.data_transacao as datetime_transacao,
            im.id_tipo_modal,
            im.id_consorcio,
            im.id_operadora,
            im.id_linha,
            im.id_transacao,
            im.sentido,
            im.perc_rateio,
            im.valor_rateio_compensacao,
            im.valor_rateio,
            im.valor_transacao,
            i.valor_transacao_total,
            i.tx_adicional as texto_adicional
        from
            integracao_transacao_deduplicada i,
            -- Transforma colunas com os dados de cada transação da integração em
            -- linhas diferentes
            unnest(
                [
                    {% for n in range(var("quantidade_integracoes_max")) %}
                        struct(
                            {% for column, column_config in var(
                                "colunas_integracao"
                            ).items() %}
                                {% if column_config.select %}
                                    {{ column }}_t{{ n }} as {{ column }},
                                {% endif %}
                            {% endfor %}
                            {{ n + 1 }} as sequencia_integracao
                        )
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
                ]
            ) as im
    ),
    integracao_new as (
        select
            i.data,
            i.hora,
            i.datetime_processamento_integracao,
            i.datetime_captura,
            i.datetime_transacao,
            timestamp_diff(
                i.datetime_transacao,
                lag(i.datetime_transacao) over (
                    partition by i.id_integracao order by sequencia_integracao
                ),
                minute
            ) as intervalo_integracao,
            i.id_integracao,
            i.sequencia_integracao,
            m.modo,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            i.id_linha as id_servico_jae,
            l.nr_linha as servico_jae,
            l.nm_linha as descricao_servico_jae,
            i.id_transacao,
            i.sentido,
            i.perc_rateio as percentual_rateio,
            i.valor_rateio_compensacao,
            i.valor_rateio,
            i.valor_transacao,
            i.valor_transacao_total,
            i.texto_adicional,
            '{{ var("version") }}' as versao
        from integracao_melt i
        left join
            {{ source("cadastro", "modos") }} m
            on i.id_tipo_modal = m.id_modo
            and m.fonte = "jae"
        left join {{ ref("operadoras") }} do on i.id_operadora = do.id_operadora_jae
        {# `rj-smtr.cadastro.operadoras` do on i.id_operadora = do.id_operadora_jae #}
        left join {{ ref("consorcios") }} dc on i.id_consorcio = dc.id_consorcio_jae
        {# `rj-smtr.cadastro.consorcios` dc on i.id_consorcio = dc.id_consorcio_jae #}
        left join
            {{ ref("staging_linha") }} l
            {# `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.linha` l #}
            on i.id_linha = l.cd_linha
        where i.id_transacao is not null
    ),
    complete_partitions as (
        select *, 0 as priority
        from integracao_new

        {% if is_incremental() %}
            union all

            select *, 1 as priority
            from {{ this }}
            where
                {% if integracao_partitions | length > 0 %}
                    data in ({{ integracao_partitions | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        {% endif %}
    ),
    integracoes_teste_invalidas as (
        select distinct i.id_integracao
        from complete_partitions i
        left join
            {{ ref("staging_linha_sem_ressarcimento") }} l
            {# `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.linha_sem_ressarcimento` l #}
            on i.id_servico_jae = l.id_linha
        where l.id_linha is not null or i.data < "2023-07-17"
    )
select * except (priority)
from complete_partitions
where id_integracao not in (select id_integracao from integracoes_teste_invalidas)
qualify
    row_number() over (
        partition by id_integracao, id_transacao, priority
        order by datetime_processamento_integracao desc
    )
    = 1