{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('start_date')}}") and date("{{var('end_date')}}")
            and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

with
    viagem_temperatura as (
        select
            data,
            id_veiculo,
            ano_fabricacao,
            safe_cast(
                json_value(indicadores, '$.indicador_ar_condicionado.valor') as bool
            ) as indicador_ar_condicionado,
            safe_cast(
                json_value(
                    indicadores,
                    '$.indicador_ar_condicionado.data_processamento_licenciamento'
                ) as date
            ) as data_processamento_licenciamento,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_variacao.valor'
                ) as bool
            ) as indicador_temperatura_variacao,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_transmitida.valor'
                ) as bool
            ) as indicador_temperatura_transmitida,
            safe_cast(
                json_value(
                    indicadores,
                    '$.indicador_temperatura_descartada.percentual_temperatura_nula_descartada'
                ) as numeric
            ) as percentual_temperatura_nula_descartada,
            safe_cast(
                json_value(
                    indicadores,
                    '$.indicador_temperatura_descartada.percentual_temperatura_atipica_descartada'
                ) as numeric
            ) as percentual_temperatura_atipica_descartada,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_descartada.valor'
                ) as bool
            ) as indicador_temperatura_descartada,
            safe_cast(
                json_value(
                    indicadores,
                    '$.indicador_temperatura_descartada.datetime_apuracao_subsidio'
                ) as datetime
            ) as datetime_apuracao_subsidio,
        from {{ ref("aux_viagem_temperatura") }}
        where {{ incremental_filter }}
    ),
    indicador_veiculo as (
        select distinct
            data,
            id_veiculo,
            ano_fabricacao,
            indicador_ar_condicionado,
            data_processamento_licenciamento,
            date(datetime_apuracao_subsidio) as data_verificacao_regularidade,
            indicador_temperatura_variacao,
            indicador_temperatura_transmitida,
            percentual_temperatura_nula_descartada,
            percentual_temperatura_atipica_descartada,
            indicador_temperatura_descartada
        from viagem_temperatura
        where indicador_ar_condicionado
    ),
    {% if is_incremental() %}
        dia_anterior as (
            select id_veiculo, quantidade_dia_falha_operacional
            from {{ this }}
            where data = date_sub(date("{{ var('start_date') }}"), interval 1 day)
        ),
        veiculos as (
            select distinct id_veiculo
            from viagem_temperatura
            union distinct
            select distinct id_veiculo
            from dia_anterior
        ),
    {% else %} veiculos as (select distinct id_veiculo from viagem_temperatura),
    {% endif %}
    datas as (
        select data
        from
            unnest(
                generate_date_array(
                    date("{{var('start_date')}}"), date("{{var('end_date')}}")
                )
            ) as data
    ),
    datas_veiculos as (select data, id_veiculo from datas cross join veiculos),
    falha_dia as (
        select
            d.*,
            i.* except (data, id_veiculo),
            last_value(ano_fabricacao ignore nulls) over (
                partition by id_veiculo
                order by data
                rows between unbounded preceding and 1 preceding
            ) as ano_fabricacao_anterior,
            case
                when
                    not indicador_temperatura_variacao
                    or not indicador_temperatura_transmitida
                    or indicador_temperatura_descartada
                then true
                when
                    indicador_temperatura_variacao
                    and indicador_temperatura_transmitida
                    and not indicador_temperatura_descartada
                then false
                else null
            end as indicio_falha
        from datas_veiculos as d
        left join indicador_veiculo as i using (data, id_veiculo)
    ),
    falha_consecutiva as (
        select
            *,
            countif(indicio_falha) over (
                partition by id_veiculo, grupo order by data
            ) as quantidade_dia_falha_operacional
        from
            (
                select
                    *,
                    countif(
                        not indicio_falha or ano_fabricacao != ano_fabricacao_anterior
                    ) over (
                        partition by id_veiculo
                        order by data
                        rows between unbounded preceding and current row
                    ) as grupo
                from falha_dia
            )
    ),
    {% if is_incremental() %}
        falha_consecutiva_completa as (
            select
                d.data,
                d.id_veiculo,
                f.* except (data, id_veiculo, grupo, quantidade_dia_falha_operacional),
                case
                    when grupo = 0
                    then
                        f.quantidade_dia_falha_operacional
                        + coalesce(da.quantidade_dia_falha_operacional, 0)
                    else f.quantidade_dia_falha_operacional
                end as quantidade_dia_falha_operacional
            from datas_veiculos as d
            left join falha_consecutiva as f using (data, id_veiculo)
            left join dia_anterior as da using (id_veiculo)
        ),
    {% else %}
        falha_consecutiva_completa as (
            select d.data, d.id_veiculo, f.* except (data, id_veiculo, grupo)
            from datas_veiculos as d
            left join falha_consecutiva as f using (data, id_veiculo)
        ),
    {% endif %}
    final as (
        select
            * except (quantidade_dia_falha_operacional, ano_fabricacao_anterior),
            last_value(quantidade_dia_falha_operacional ignore nulls) over (
                partition by id_veiculo
                order by data
                rows between unbounded preceding and current row
            ) as quantidade_dia_falha_operacional,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            "{{ var('version') }}" as versao,
            '{{ invocation_id }}' as id_execucao_dbt
        from falha_consecutiva_completa
    )
select *
from final
