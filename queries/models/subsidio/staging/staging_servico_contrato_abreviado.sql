{{
    config(
        materialized="view",
        alias="servico_contrato_abreviado",
    )
}}

{% set meses = {
    "jan": "Jan",
    "fev": "Feb",
    "mar": "Mar",
    "abr": "Apr",
    "mai": "May",
    "jun": "Jun",
    "jul": "Jul",
    "ago": "Aug",
    "set": "Sep",
    "out": "Oct",
    "nov": "Nov",
    "dez": "Dec",
} %}

select
    safe_cast(`Confirmação CGR` as bool) as confirmacao_cgr,
    safe_cast(`Serviço` as string) as servico,
    safe_cast(`Empresa` as string) as empresa,
    safe_cast(`Fase` as string) as fase,
    parse_date(
        '%b./%y',
        {% for pt, en in meses.items() %} replace({% endfor %}`Data `
        {% for pt, en in meses.items() %}, '{{ pt }}', '{{ en }}') {% endfor %}
    ) as data,
    safe_cast(`Observação` as string) as observacao
from {{ source("subsidio_staging", "servico_contrato_abreviado") }}
where `Serviço` != ""
