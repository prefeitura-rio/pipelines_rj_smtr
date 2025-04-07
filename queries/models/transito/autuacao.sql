{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["id_autuacao", "status_infracao", "data_pagamento"],
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    date(data)
    between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            select distinct concat("'", date(data_autuacao), "'") as partition_date
            from
                (
                    select distinct data_autuacao
                    from {{ ref("autuacao_citran") }}
                    where {{ incremental_filter }}
                    union all
                    select distinct data_autuacao
                    from {{ ref("autuacao_serpro") }}
                    where {{ incremental_filter }}
                )
        {% endset %}
        {% set partitions = run_query(partitions_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    infracoes_renainf as (
        select
            concat(codigo_infracao, desdobramento) as codigo_enquadramento,
            descricao_infracao as tipificacao_resumida
        from {{ source("autuacao_staging", "infracoes_renainf") }}
    ),
    autuacao_ids as (
        select data, id_autuacao, id_auto_infracao, fonte
        from {{ ref("aux_autuacao_id") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    citran as (
        select
            data_autuacao as data,
            id_auto_infracao,
            datetime(concat(data, ' ', hora, ':00')) as datetime_autuacao,
            data_limite_defesa_previa,
            data_limite_recurso,
            situacao_atual as descricao_situacao_autuacao,
            if(status_infracao != "", status_infracao, null) as status_infracao,
            replace(codigo_enquadramento, '-', '') as codigo_enquadramento,
            safe_cast(
                substr(regexp_extract(pontuacao, r'\d+'), 2) as string
            ) as pontuacao,
            case
                when initcap(regexp_replace(pontuacao, r'\d+', '')) = 'Media'
                then 'Média'
                when initcap(regexp_replace(pontuacao, r'\d+', '')) = 'Gravissima'
                then 'Gravíssima'
                else initcap(regexp_replace(pontuacao, r'\d+', ''))
            end as gravidade,
            safe_cast(null as string) as amparo_legal,
            initcap(tipo_veiculo) as tipo_veiculo,
            if(descricao_veiculo != "", descricao_veiculo, null) as descricao_veiculo,
            safe_cast(null as string) as placa_veiculo,
            safe_cast(null as string) as ano_fabricacao_veiculo,
            safe_cast(null as string) as ano_modelo_veiculo,
            safe_cast(null as string) as cor_veiculo,
            case
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Misto', '0Misto')
                then 'Misto'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Passageir', '0Passageir', 'Passageiro', '0Passageiro')
                then 'Passageiro'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Tracao', '0Tracao', 'Tracao')
                then 'Tração'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Nao Inform', '0Nao Inform', 'Nao Informado', '0Nao Informado')
                then 'Não informado'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Carga', '0Carga')
                then 'Carga'
                else 'Inválido'
            end as especie_veiculo,
            safe_cast(null as string) as uf_infrator,
            safe_cast(null as string) as uf_principal_condutor,
            if(uf_proprietario != "", uf_proprietario, null) as uf_proprietario,
            if(cep_proprietario != "", cep_proprietario, null) as cep_proprietario,
            valor_infracao / 100 as valor_infracao,
            valor_pago / 100 as valor_pago,
            data_pagamento,
            "260010" as id_autuador,
            if(
                descricao_autuador != "", descricao_autuador, null
            ) as descricao_autuador,
            "6001" as id_municipio_autuacao,
            "RIO DE JANEIRO" as descricao_municipio,
            "RJ" as uf_autuacao,
            endereco_autuacao as cep_autuacao,
            null as tile_autuacao,
            if(
                processo_defesa_autuacao != "00000000"
                and processo_defesa_autuacao != "",
                processo_defesa_autuacao,
                null
            ) as processo_defesa_autuacao,
            if(
                recurso_penalidade_multa != "00000000"
                and recurso_penalidade_multa != "",
                recurso_penalidade_multa,
                null
            ) as recurso_penalidade_multa,
            if(
                processo_troca_real_infrator != "00000000"
                and processo_troca_real_infrator != "",
                processo_troca_real_infrator,
                null
            ) as processo_troca_real_infrator,
            false as status_sne,
            "CITRAN" as fonte,
            datetime("2023-08-26") as datetime_ultima_atualizacao
        from {{ ref("autuacao_citran") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    serpro as (
        select
            data_autuacao as data,
            id_auto_infracao,
            datetime_autuacao,
            data_limite_defesa_previa,
            data_limite_recurso,
            descricao_situacao_autuacao,
            if(status_infracao != "", status_infracao, null) as status_infracao,
            concat(codigo_enquadramento, codigo_desdobramento) as codigo_enquadramento,
            substr(pontuacao, 1, 1) as pontuacao,
            gravidade,
            amparo_legal,
            initcap(tipo_veiculo) as tipo_veiculo,
            if(descricao_veiculo != "", descricao_veiculo, null) as descricao_veiculo,
            placa_veiculo,
            ano_fabricacao_veiculo,
            ano_modelo_veiculo,
            cor_veiculo,
            especie_veiculo,
            uf_infrator,
            uf_principal_condutor,
            if(uf_proprietario != "", uf_proprietario, null) as uf_proprietario,
            safe_cast(null as string) as cep_proprietario,
            valor_infracao,
            valor_pago,
            data_pagamento,
            coalesce(id_autuador, "260010") as id_autuador,
            if(
                descricao_autuador != "", descricao_autuador, null
            ) as descricao_autuador,
            coalesce(id_municipio_autuacao, "6001") as id_municipio_autuacao,
            coalesce(descricao_municipio, "RIO DE JANEIRO") as descricao_municipio,
            coalesce(uf_autuacao, "RJ") as uf_autuacao,
            case
                when logradouro_autuacao is not null
                then
                    rtrim(
                        regexp_replace(
                            concat(
                                logradouro_autuacao,
                                ' ',
                                bairro_autuacao,
                                ' ',
                                complemento
                            ),
                            r'\s+',
                            ' '
                        )
                    )
                when logradouro_rodovia_autuacao is not null
                then logradouro_rodovia_autuacao
                else null
            end as cep_autuacao,
            null as tile_autuacao,
            processo_defesa_autuacao,
            recurso_penalidade_multa,
            processo_troca_real_infrator,
            if(status_sne = "1.0", true, false) as status_sne,
            "SERPRO" as fonte,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from {{ ref("autuacao_serpro") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    autuacao as (
        select *
        from citran
        union all
        select *
        from serpro
    ),
    update_partition as (
        select a.*
        from autuacao a
        {% if is_incremental() %}
            left join
                {{ this }} as t
                on a.id_auto_infracao = t.id_auto_infracao
                and a.fonte = t.fonte
                and t.data in ({{ partitions | join(", ") }})
            where
                t.id_auto_infracao is null
                or a.status_infracao != t.status_infracao
                or a.data_pagamento != t.data_pagamento
        {% endif %}
    )
select
    u.data,
    a.id_autuacao,
    u.id_auto_infracao,
    datetime_autuacao,
    data_limite_defesa_previa,
    data_limite_recurso,
    descricao_situacao_autuacao,
    status_infracao,
    codigo_enquadramento,
    i.tipificacao_resumida,
    pontuacao,
    gravidade,
    amparo_legal,
    tipo_veiculo,
    descricao_veiculo,
    placa_veiculo,
    ano_fabricacao_veiculo,
    ano_modelo_veiculo,
    cor_veiculo,
    especie_veiculo,
    uf_infrator,
    uf_principal_condutor,
    uf_proprietario,
    cep_proprietario,
    valor_infracao,
    valor_pago,
    data_pagamento,
    id_autuador,
    descricao_autuador,
    id_municipio_autuacao,
    descricao_municipio,
    uf_autuacao,
    cep_autuacao,
    tile_autuacao,
    processo_defesa_autuacao,
    recurso_penalidade_multa,
    processo_troca_real_infrator,
    status_sne,
    fonte,
    datetime_ultima_atualizacao
from update_partition as u
left join autuacao_ids as a using (data, id_auto_infracao, fonte)
left join infracoes_renainf as i using (codigo_enquadramento)
