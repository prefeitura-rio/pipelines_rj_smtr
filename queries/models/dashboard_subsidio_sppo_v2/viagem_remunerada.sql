{{
    config(
        materialized="view",
    )
}}

select *
from {{ ref("subsidio_viagem_remunerada") }}
where
    data between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    and modo = "Ônibus SPPO"

    -- renomear valor_apurado para valor_pago
    -- add motivos de não remuneração
    -- indicadores: limite, pof < 80, tecnologia inferior minima,
    -- indicador_viagem_remunerada
    -- valor_total_sem_glosa - valor_glosado_tecnologia as valor_total_sem_glosa,

