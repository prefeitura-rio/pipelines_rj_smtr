{{
    config(
        materialized="view",
    )
}}

select
    data,
    linha,
    round(avg(n_veiculos)) as frota_operante_media,
    case
        when hora between 5 and 8 then "manhã" when hora between 16 and 19 then "noite"
    end as pico,
from `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_agg_data_hora_linha` as t
where (t.hora between 5 and 8 or t.hora between 16 and 19)
group by pico, data, linha
order by linha
