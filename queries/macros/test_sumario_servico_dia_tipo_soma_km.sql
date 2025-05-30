{% test sumario_servico_dia_tipo_soma_km(model, column_name) -%}
    with
        kms as (
            select
                * except ({{ column_name }}),
                {{ column_name }},
                round(
                    coalesce(km_apurada_registrado_com_ar_inoperante, 0)
                    + coalesce(km_apurada_n_licenciado, 0)
                    + coalesce(km_apurada_autuado_ar_inoperante, 0)
                    + coalesce(km_apurada_autuado_seguranca, 0)
                    + coalesce(km_apurada_autuado_limpezaequipamento, 0)
                    {% if "faixa" in model %}
                        + coalesce(km_apurada_total_licenciado_sem_ar_n_autuado, 0)
                        + coalesce(km_apurada_total_licenciado_com_ar_n_autuado, 0)
                    {% else %}
                        + coalesce(km_apurada_licenciado_sem_ar_n_autuado, 0)
                        + coalesce(km_apurada_licenciado_com_ar_n_autuado, 0)
                    {% endif %}
                    + coalesce(km_apurada_n_vistoriado, 0)
                    + coalesce(km_apurada_sem_transacao, 0),
                    2
                ) as km_apurada2
            from {{ model }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        )
    select *, abs(km_apurada2 -{{ column_name }}) as dif
    from kms
    where abs(km_apurada2 -{{ column_name }}) > 0.02
{%- endtest %}
