{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}
with
    pre_faixa_horaria as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            tipo_viagem,
            indicador_ar_condicionado,
            viagens,
            km_apurada
        from {{ ref("sumario_servico_tipo_viagem_dia") }}
        -- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_tipo_viagem_dia`
        where data < date("{{ var(" data_subsidio_v9_inicio ") }}")
				{% if is_incremental() %}
						AND data BETWEEN DATE("{{ var("start_date") }}")
						AND DATE_ADD(DATE("{{ var("end_date") }}"), INTERVAL 1 DAY)
				{% endif %}
    ),
    pos_faixa_horaria as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            tipo_viagem,
            indicador_ar_condicionado,
            sum(viagens_faixa) as viagens,
            sum(km_apurada_faixa) as km_apurada
        from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
        -- `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem`
        where
            data >= date("{{ var(" data_subsidio_v9_inicio ") }}")
            and tipo_viagem != "Sem viagem apurada"
						{% if is_incremental() %}
								AND data BETWEEN DATE("{{ var("start_date") }}")
								AND DATE_ADD(DATE("{{ var("end_date") }}"), INTERVAL 1 DAY)
						{% endif %}
        group by
            data, tipo_dia, consorcio, servico, tipo_viagem, indicador_ar_condicionado
    )
select *
from pre_faixa_horaria
union all
select *
from pos_faixa_horaria
