{{
    config(
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set integracao_table = ref("integracao") %}
{% if execute %}
    {% if is_incremental() %}

        {% set partitions_query %}
      SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
      FROM
        `{{ integracao_table.database }}.{{ integracao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        {# `rj-smtr.br_rj_riodejaneiro_bilhetagem.INFORMATION_SCHEMA.PARTITIONS` #}
      WHERE
        table_name = "{{ integracao_table.identifier }}"
        AND partition_id != "__NULL__"
        AND DATE(last_modified_time, "America/Sao_Paulo") BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
        {% endset %}

        {{ log("Running query: \n" ~ partitions_query, info=True) }}
        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
        {{ log("integracao partitions: \n" ~ partition_list, info=True) }}
    {% endif %}
{% endif %}

with
    matriz as (
        select distinct
            data_inicio_matriz,
            data_fim_matriz,
            array_to_string(sequencia_completa_modo, ', ') as modos,
            to_json_string(sequencia_completa_rateio) as rateio,
            tempo_integracao_minutos
        from {{ ref("matriz_integracao") }}
    ),
    versao_matriz as (select distinct data_inicio_matriz, data_fim_matriz from matriz),
    integracao_agg as (
        select
            date(datetime_processamento_integracao) as data,
            id_integracao,
            string_agg(
                case
                    when modo = 'Van'
                    then consorcio
                    when modo = 'Ônibus'
                    then 'SPPO'
                    else modo
                end,
                ', '
                order by sequencia_integracao
            ) as modos,
            to_json_string(
                array_agg(
                    cast(percentual_rateio as numeric) order by sequencia_integracao
                )
            ) as rateio,
            min(datetime_transacao) as datetime_primeira_transacao,
            max(datetime_transacao) as datetime_ultima_transacao,
            min(intervalo_integracao) as menor_intervalo
        from {{ ref("integracao") }}
        {# from `rj-smtr.br_rj_riodejaneiro_bilhetagem.integracao` #}
        {% if is_incremental() %}
            where
                {% if partition_list | length > 0 %}
                    data in ({{ partition_list | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        {% endif %}
        group by 1, 2
    ),
    integracao_matriz as (
        select
            i.data,
            i.id_integracao,
            i.modos,
            i.rateio,
            i.datetime_primeira_transacao,
            i.datetime_ultima_transacao,
            i.menor_intervalo,
            m.modos as modos_matriz,
            m.rateio as rateio_matriz,
            m.tempo_integracao_minutos,
            v.data_inicio_matriz
        from integracao_agg i
        left join
            matriz m
            on i.data >= m.data_inicio_matriz
            and (i.data <= m.data_fim_matriz or m.data_fim_matriz is null)
            and i.modos = m.modos
        left join
            versao_matriz v
            on i.data >= v.data_inicio_matriz
            and (i.data <= v.data_fim_matriz or v.data_fim_matriz is null)
    ),
    indicadores as (
        select
            data,
            id_integracao,
            modos,
            modos_matriz is null as indicador_fora_matriz,
            case
                when modos_matriz is null
                then null
                else
                    timestamp_diff(
                        datetime_ultima_transacao, datetime_primeira_transacao, minute
                    )
                    > tempo_integracao_minutos
            end as indicador_tempo_integracao_invalido,
            case
                when modos_matriz is null then null else rateio != rateio_matriz
            end as indicador_rateio_invalido,
            rateio,
            rateio_matriz,
            data_inicio_matriz
        from integracao_matriz
    )
select
    *,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from indicadores
where
    (
        indicador_fora_matriz
        or indicador_tempo_integracao_invalido
        or indicador_rateio_invalido
    )
    and data >= (select min(data_inicio_matriz) from versao_matriz)
