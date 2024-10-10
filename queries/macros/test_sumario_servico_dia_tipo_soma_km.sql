{% test sumario_servico_dia_tipo_soma_km(model, column_name) -%}
WITH
    kms AS (
    SELECT
        * EXCEPT({{ column_name }}),
        {{ column_name }},
        ROUND(COALESCE(km_apurada_registrado_com_ar_inoperante,0) + COALESCE(km_apurada_n_licenciado,0) + COALESCE(km_apurada_autuado_ar_inoperante,0) + COALESCE(km_apurada_autuado_seguranca,0) + COALESCE(km_apurada_autuado_limpezaequipamento,0) + COALESCE(km_apurada_licenciado_sem_ar_n_autuado,0) + COALESCE(km_apurada_licenciado_com_ar_n_autuado,0) + COALESCE(km_apurada_n_vistoriado, 0) + COALESCE(km_apurada_sem_transacao, 0),2) AS km_apurada2
    FROM
        {{ model }}
    WHERE
        DATA BETWEEN DATE("{{ var('start_timestamp') }}")
        AND DATE("{{ var('end_timestamp') }}"))
SELECT
    *,
    ABS(km_apurada2-{{ column_name }}) AS dif
FROM
    kms
WHERE
    ABS(km_apurada2-{{ column_name }}) > 0.02
{%- endtest %}