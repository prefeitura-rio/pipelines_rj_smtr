{{
    config(
        materialized="ephemeral",
    )
}}
select * from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
 where
 data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
     {# data between date_sub(date("{{ var("run_date") }}"), interval 1 day) and date("{{ var("run_date") }}") #}