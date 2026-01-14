{{
    config(
        materialized="table",
    )
}}

select 
data_inicio,
data_fim,
perc_km_inferior,
perc_km_superior,
tipo_penalidade, 
valor,
datetime_ultima_atualizacao,
versao,
id_execucao_dbt
from {{ ref("staging_valor_tipo_penalidade") }}
