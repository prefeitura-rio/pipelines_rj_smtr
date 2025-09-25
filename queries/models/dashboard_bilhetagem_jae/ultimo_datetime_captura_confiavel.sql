with
    verificao as (
        select
            timestamp_captura,
            min(indicador_captura_correta) as indicador_captura_correta
        from {{ source("source_jae", "resultado_verificacao_captura_jae") }}
        where table_id in ('transacao', 'transacao_riocard')
        group by 1
    ),
    ts as (
        select datetime(timestamp_captura, "America/Sao_Paulo") as timestamp_captura
        from
            unnest(
                generate_timestamp_array(
                    (
                        select min(timestamp(timestamp_captura, 'America/Sao_Paulo'))
                        from verificao
                    ),
                    current_timestamp(),
                    interval 1 minute
                )
            ) timestamp_captura
    ),
    ts_filtrado as (
        select timestamp_captura
        from ts
        left join verificao using (timestamp_captura)
        qualify
            sum(ifnull(cast(not indicador_captura_correta as integer), 1)) over (
                order by timestamp_captura
            )
            = 0
    )
select max(timestamp_captura) as datetime_confiavel
from ts_filtrado
