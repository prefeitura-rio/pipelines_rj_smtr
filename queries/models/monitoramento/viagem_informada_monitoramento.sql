{{
    config(
        alias="viagem_informada",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

select
    data,
    id_viagem,
    datetime_partida,
    datetime_chegada,
    id_veiculo,
    trip_id,
    null as route_id,
    shape_id,
    servico_informado as servico,
    sentido,
    datetime_chegada as datetime_processamento,
    datetime_chegada as datetime_captura,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from
    {# {{ ref('viagem_completa') }} #}
    `rj-smtr.projeto_subsidio_sppo.viagem_completa`
{% if is_incremental() %}
    where data = date_sub(date('{{ var("run_date") }}'), interval 1 day)
{% endif %}
