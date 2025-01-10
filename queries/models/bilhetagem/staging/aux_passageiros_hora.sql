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
    cadastro_cliente,
    produto,
    tipo_transacao,
    tipo_usuario,
    meio_pagamento,
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
    "Não Cadastrado" as cadastro_cliente,
    "RioCard" as produto,
    "RioCard" as tipo_transacao,
    "RioCard" as tipo_usuario,
    "RioCard" as meio_pagamento,
    st_geogpoint(longitude, latitude) as geo_point_transacao
{# from {{ ref("transacao_riocard") }} #}
from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao_riocard`
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
