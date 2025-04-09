select
    date(usage_start_time) as data,
    service.description as tipo_servico,
    sku.description as sku,
    project.name as projeto,
    cost as custo_real,
    currency_conversion_rate as taxa_conversao_para_real,
    usage.amount as quantidade_uso,
    usage.unit as unidade_uso,
    usage.amount_in_pricing_units as uso_quantidade_em_unidades_preco,
    usage.pricing_unit as unidade_de_preco_de_uso,
from {{ source("cloudcosts", "gcp_billing_export_resource_v1_010693_B1EC8D_C0D4DF") }}
where usage_start_time >= "2024-10-01"
