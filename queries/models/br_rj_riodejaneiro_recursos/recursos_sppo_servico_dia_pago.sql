{{
    config(
        materialized="table",
    )
}}

with
    -- 1. Trata os IDs de recurso de forma que, caso tenha mais que um, sempre estejam
    -- separados por vírgula, ordenados do menor para o maior e sem espaços adicionais
    treated_id_recurso as (
        select distinct *
        from
            (
                select
                    id_recurso_old,
                    array_to_string(
                        array(select trim(id) from unnest(id_array) as id order by id),
                        ", "
                    ) as id_recurso
                from
                    (
                        select
                            id_recurso as id_recurso_old,
                            split(replace(id_recurso, " e ", ", "), ", ") as id_array
                        from
                            {{
                                source(
                                    "br_rj_riodejaneiro_recursos_staging",
                                    "recursos_sppo_servico_dia_pago",
                                )
                            }}
                    )
            )
    ),
    -- 2. Trata a tabela em staging
    treated_recurso as (
        select
            * except (
                data, valor_pago, tipo_dia, incorporado_algoritmo, pagamento_media
            ),
            parse_date("%d/%m/%Y", data) as data,
            safe_cast(
                regexp_replace(
                    regexp_replace(regexp_replace(valor_pago, r"\.", ""), r",", "."),
                    r"[^\d\.-]",
                    ""
                ) as float64
            ) as valor_pago,
            case
                when tipo_dia like "R$%" or tipo_dia = "" then null else tipo_dia
            end as tipo_dia,
            case
                when trim(incorporado_algoritmo) = "Sim" then true else false
            end as incorporado_algoritmo,
            case
                when trim(pagamento_media) = "Sim" then true else false
            end as pagamento_media,
        from
            {{
                source(
                    "br_rj_riodejaneiro_recursos_staging",
                    "recursos_sppo_servico_dia_pago",
                )
            }}
        where data not like "%-%"
    )
select
    data,
    tipo_dia,
    i.id_recurso,
    tipo_recurso,
    quinzena_ocorrencia,
    quinzena_pagamento,
    consorcio,
    servico,
    valor_pago,
    incorporado_algoritmo,
    pagamento_media,
    motivo
from treated_recurso as t
left join treated_id_recurso as i on t.id_recurso = i.id_recurso_old
