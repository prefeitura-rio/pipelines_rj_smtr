select
    min(data_inicio) as data_inicio,
    max(data_fim) as data_fim,
    irk,
    irk_tarifa_publica,
    max(subsidio_km) as subsidio_km,
from {{ ref("valor_km_tipo_viagem") }}
where data_inicio >= "{{ var('encontro_contas_datas_v2_inicio') }}"
group by all
order by data_inicio
