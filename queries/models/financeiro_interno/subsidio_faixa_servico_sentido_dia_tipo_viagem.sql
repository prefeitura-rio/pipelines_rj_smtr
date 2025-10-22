{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    viagem_remunerada as (
        select *
        from {{ ref("subsidio_viagem_remunerada") }}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    )
select
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    sentido,
    modo,
    indicador_ar_condicionado,
    indicador_penalidade_judicial,
    indicador_viagem_dentro_limite,
    tipo_viagem,
    tecnologia_apurada,
    tecnologia_remunerada,
    subsidio_km,
    subsidio_km_teto,
    coalesce(count(id_viagem), 0) as viagens_faixa,
    coalesce(sum(km_apurada), 0) as km_apurada_faixa,
    coalesce(sum(km_subsidiada), 0) as km_subsidiada_faixa,
    coalesce(sum(valor_apurado), 0) as valor_apurado,
    coalesce(sum(valor_glosado_tecnologia), 0) as valor_glosado_tecnologia,
    coalesce(sum(valor_acima_limite), 0) as valor_acima_limite,
    coalesce(sum(valor_sem_glosa), 0) as valor_total_sem_glosa,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from viagem_remunerada
group by
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    sentido,
    modo,
    indicador_ar_condicionado,
    indicador_penalidade_judicial,
    indicador_viagem_dentro_limite,
    tipo_viagem,
    tecnologia_apurada,
    tecnologia_remunerada,
    subsidio_km,
    subsidio_km_teto
