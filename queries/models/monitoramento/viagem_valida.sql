{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

with
    veiculo as (
        select *
        from {{ ref("status_dia") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    viagem_valida as (
        select *
        {# from {{ ref("viagem_validacao") }} #}
        from `rj-smtr.monitoramento.viagem_validacao` v
        where
            indicador_viagem_valida
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    )
select
    vv.data,
    vv.id_viagem,
    vv.datetime_partida,
    vv.datetime_chegada,
    vv.modo,
    vv.id_veiculo,
    ve.tecnologia as tecnologia_apurada,
    ve.status as tipo_viagem,
    vv.servico,
    vv.sentido,
    vv.distancia_planejada,
    vv.feed_start_date,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_valida as vv
left join veiculo as ve using (data, id_veiculo)
