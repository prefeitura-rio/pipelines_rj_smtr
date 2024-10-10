{{ config(materialized="ephemeral") }}

select
    data,
    hora,
    modo,
    consorcio,
    id_servico_jae,
    servico_jae,
    descricao_servico_jae,
    sentido,
    id_transacao,
    tipo_transacao_smtr,
    ifnull(
        case
            when tipo_transacao_smtr = "Gratuidade"
            then tipo_gratuidade
            when tipo_transacao_smtr = "Integração"
            then "Integração"
            when tipo_transacao_smtr = "Transferência"
            then "Transferência"
            else tipo_pagamento
        end,
        "Não Identificado"
    ) as tipo_transacao_detalhe_smtr,
    tipo_gratuidade,
    tipo_pagamento,
    geo_point_transacao
from {{ ref("transacao") }}
where
    id_servico_jae not in ("140", "142")
    and id_operadora != "2"
    and (
        modo = "BRT"
        or (modo = "VLT" and data >= date("2024-02-24"))
        or (modo = "Ônibus" and data >= date("2024-04-19"))
        or (modo = "Van" and consorcio = "STPC" and data >= date("2024-07-01"))
        or (modo = "Van" and consorcio = "STPL" and data >= date("2024-07-15"))
    )
    and tipo_transacao is not null

union all

select
    data,
    hora,
    modo,
    consorcio,
    id_servico_jae,
    servico_jae,
    descricao_servico_jae,
    sentido,
    id_transacao,
    "RioCard" as tipo_transacao_smtr,
    "RioCard" as tipo_transacao_detalhe_smtr,
    null as tipo_gratuidade,
    "RioCard" as tipo_pagamento,
    st_geogpoint(longitude, latitude) as geo_point_transacao
from {{ ref("transacao_riocard") }}
where
    (id_servico_jae not in ("140", "142") or id_servico_jae is null)
    and (id_operadora != "2" or id_operadora is null)
    and (
        modo = "BRT"
        or (modo = "VLT" and data >= date("2024-02-24"))
        or (modo = "Ônibus" and data >= date("2024-04-19"))
        or (modo = "Van" and consorcio = "STPC" and data >= date("2024-07-01"))
        or (modo = "Van" and consorcio = "STPL" and data >= date("2024-07-15"))
        or modo is null
    )
