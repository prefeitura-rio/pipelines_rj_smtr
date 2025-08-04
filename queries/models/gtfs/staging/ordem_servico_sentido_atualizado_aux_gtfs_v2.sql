/*
  ordem_servico_gtfs com sentidos despivotados, ajustes nos horários e com atualização dos sentidos circulares
*/
{{ config(materialized="ephemeral") }}

select
    * except (
        sentido,
        extensao,
        datetime_ultima_atualizacao,
        id_execucao_dbt,
        versao,
        quilometragem,
        faixa_horaria_inicio,
        faixa_horaria_fim
    ),
    left(sentido, 1) as sentido,
    extensao as distancia_planejada,
    partidas as viagens_planejadas,
    quilometragem as distancia_total_planejada,
    cast(null as string) as inicio_periodo,
    cast(null as string) as fim_periodo
from {{ ref("ordem_servico_faixa_horaria_sentido") }}
