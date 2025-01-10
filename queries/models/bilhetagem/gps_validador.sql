{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        tags=["geolocalizacao"],
    )
}}

select
    modo,
    extract(date from datetime_gps) as data,
    extract(hour from datetime_gps) as hora,
    datetime_gps,
    datetime_captura,
    id_operadora,
    operadora,
    id_servico_jae,
    -- s.servico,
    servico_jae,
    descricao_servico_jae,
    case
        when modo = "VLT"
        then substring(id_veiculo, 1, 3)
        when modo = "BRT"
        then null
        else id_veiculo
    end as id_veiculo,
    id_validador,
    id_transmissao_gps,
    latitude,
    longitude,
    sentido,
    estado_equipamento,
    temperatura,
    versao_app,
    '{{ var("version") }}' as versao
from
    (
        select
            *,
            row_number() over (
                partition by id_transmissao_gps order by datetime_captura desc
            ) as rn
        from {{ ref("aux_gps_validador") }}

        {% if is_incremental() %}
            where
                date(data) between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and datetime_captura > datetime("{{var('date_range_start')}}")
                and datetime_captura <= datetime("{{var('date_range_end')}}")
        {% endif %}
    )
where rn = 1 and modo != "Van"
