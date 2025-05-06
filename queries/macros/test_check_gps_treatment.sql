{% test check_gps_treatment(model) -%}
    with
        data_hora as (
            select
                extract(date from timestamp_array) as data,
                extract(hour from timestamp_array) as hora,
            from
                unnest(
                    generate_timestamp_array(
                        "{{ var('date_range_start') }}",
                        "{{ var('date_range_end') }}",
                        interval 1 hour
                    )
                ) as timestamp_array
        ),
        gps as (
            select data, timestamp_gps, latitude, longitude
            from {{ ref('sppo_registros') }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            qualify row_number() over (partition by ordem, timestamp_gps, linha) = 1
        ),
        gps_raw as (
            select
                extract(date from timestamp_gps) as data,
                extract(hour from timestamp_gps) as hora,
                count(*) as q_gps_raw
            from gps
            group by 1, 2
        ),
        gps_filtrada as (
            select
                extract(date from timestamp_gps) as data,
                extract(hour from timestamp_gps) as hora,
                count(*) as q_gps_filtrada
            from
                -- `rj-smtr.br_rj_riodejaneiro_onibus_gps.sppo_aux_registros_filtrada`
                {{ ref("sppo_aux_registros_filtrada") }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            group by 1, 2
        ),
        gps_sppo as (
            select
                data,
                extract(hour from timestamp_gps) as hora,
                count(*) as q_gps_treated
            from
                -- `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
                {{ ref("gps_sppo") }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            group by 1, 2
        ),
        gps_join as (
            select
                *,
                safe_divide(q_gps_filtrada, q_gps_raw) as indice_tratamento_raw,
                safe_divide(
                    q_gps_treated, q_gps_filtrada
                ) as indice_tratamento_filtrada,
                case
                    when
                        q_gps_raw = 0 or q_gps_filtrada = 0 or q_gps_treated = 0  -- Hipótese de perda de dados no tratamento
                        or q_gps_raw is null or q_gps_filtrada is null or q_gps_treated is null -- Hipótese de perda de dados no tratamento
                        or (q_gps_raw <= q_gps_filtrada) or (q_gps_filtrada < q_gps_treated) -- Hipótese de duplicação de dados
                        or (coalesce(safe_divide(q_gps_filtrada, q_gps_raw), 0) < 0.96) -- Hipótese de perda de dados no tratamento (superior a 3%)
                        or (coalesce(safe_divide(q_gps_treated, q_gps_filtrada), 0) < 0.96) -- Hipótese de perda de dados no tratamento (superior a 3%)
                    then false
                    else true
                end as status
            from data_hora
            left join gps_raw using (data, hora)
            left join gps_filtrada using (data, hora)
            left join gps_sppo using (data, hora)
        )
    select *
    from gps_join
    where status is false
{%- endtest %}
