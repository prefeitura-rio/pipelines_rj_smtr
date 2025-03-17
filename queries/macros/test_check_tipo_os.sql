{% test check_tipo_os(model) -%}
-- mudar para dbt.expectations
    WITH 
    viagem_planejada AS (
        SELECT distinct data, tipo_dia
        FROM {{ ref('viagem_planejada') }}
        WHERE data = date_sub(current_date(), interval 1 day)
    ),
    data_versao_efetiva AS (
        SELECT distinct data, concat(concat(tipo_dia, " - "), subtipo_dia) as tipo_dia
        FROM {{ ref('subsidio_data_versao_efetiva') }}
        WHERE data = date_sub(current_date(), interval 1 day)
    )
SELECT
    *
FROM viagem_planejada p full join data_versao_efetiva dve using data
WHERE p.tipo_dia is distinct from dve.tipo_dia
{%- endtest %}
