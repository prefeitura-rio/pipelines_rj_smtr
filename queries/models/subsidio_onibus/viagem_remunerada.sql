{{
    config(
        materialized="view",
    )
}}

select
    data,
    tipo_dia,
    subtipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    modo,
    id_veiculo,
    id_viagem,
    datetime_partida,
    tipo_viagem,
    tecnologia_apurada,
    tecnologia_remunerada,
    km_apurada,
    pof,
    km_subsidiada,
    indicador_viagem_dentro_limite,
    pof < 80 as indicador_pof_inferior_80,
    indicador_penalidade_tecnologia as indicador_tecnologia_inferior_minima,
    (
        indicador_viagem_dentro_limite
        and not pof < 80
        and not indicador_penalidade_tecnologia
    ) as indicador_viagem_remunerada,
    valor_apurado as valor_pago,
    valor_glosado_tecnologia,
    valor_acima_limite,
    valor_sem_glosa,
    valor_sem_glosa - valor_glosado_tecnologia as valor_total_sem_glosa,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("subsidio_viagem_remunerada") }}
where
    data between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    and modo = "Ônibus SPPO"
