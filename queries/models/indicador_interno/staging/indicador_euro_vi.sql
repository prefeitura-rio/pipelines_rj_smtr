{{
    config(
        meta={
            "data_product": "Indicador: Percentual de Veículos Operantes Euro VI ou Superior",
            "data_product_code": "PVEUROIV",
            "ted": "TED_001-25_DTDI/SUBTT-SUBPCT",
            "chief_data_owner": "lauro.silvestre@prefeitura.rio",
            "business_data_owner": "rebeca.bittencourt@prefeitura.rio",
            "data_steward": "victormiguel@prefeitura.rio",
            "data_custodian": "rodrigo.fcunha@prefeitura.rio",
        }
    )
}}

{% set incremental_filter %}
    {% if is_incremental() %}
    data between date_trunc(date("{{ var('start_date') }}"), month) and last_day(date("{{ var('end_date') }}"), month)
    and data < date_trunc(current_date("America/Sao_Paulo"), month)
    {% else %}
    data < date_trunc(current_date("America/Sao_Paulo"), month)
    {% endif %}
{% endset %}

with
    /*
    1. viagem_completa
        Dados de viagens completas do SPPO
    */
    viagem_completa as (
        select distinct data, id_veiculo
        from {{ ref("viagem_completa") }}
        where
            data >= "{{ var('DATA_SUBSIDIO_V13_INICIO') }}"
            and data < date_trunc(current_date("America/Sao_Paulo"), month)
            and {{ incremental_filter }}
    ),
    /*
        2. veiculo_licenciamento_dia_backup
        Dados de licenciamento dos veículos por dia recuperados por backup [2025-04-01 a 2025-07-24]
        [Temporário enquanto não for reprocessado desde Abril/2025]
    */
    veiculo_licenciamento_dia_backup as (
        select data, data_processamento, id_veiculo, ano_fabricacao, placa
        from `rj-smtr-dev`.`cadastro`.`veiculo_licenciamento_dia_2025_07_24`
        where
            data
            between date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}") and date("2025-07-24")
            and {{ incremental_filter }}
    ),
    /*
        3. veiculo_licenciamento_dia_prod
        Dados de licenciamento dos veículos por dia em produção [Após 2025-07-24]
    */
    veiculo_licenciamento_dia_prod as (
        select data, data_processamento, id_veiculo, ano_fabricacao, placa
        from {{ ref("veiculo_licenciamento_dia") }}
        where
            data >= "{{ var('DATA_SUBSIDIO_V13_INICIO') }}"
            and data < date_trunc(current_date("America/Sao_Paulo"), month)
            and data > "2025-07-24"
            and {{ incremental_filter }}
    ),
    /*
        4. veiculo_licenciamento_dia
        União dos dados de licenciamento dos veículos por dia
        [veiculo_licenciamento_dia_backup + veiculo_licenciamento_dia_prod]
    */
    veiculo_licenciamento_dia as (
        select *
        from veiculo_licenciamento_dia_backup
        union all
        select *
        from veiculo_licenciamento_dia_prod
    ),
    /*
        5. veiculo_dia
        Dados diários dos veículos do SPPO
    */
    veiculo_dia as (
        select
            data,
            id_veiculo,
            tipo_veiculo,
            ano_fabricacao,
            placa,
            safe_cast(
                json_value(
                    indicadores,
                    '$.indicador_licenciado.data_processamento_licenciamento'
                ) as date
            ) as data_processamento,
        from {{ ref("veiculo_dia") }}
        where
            data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
            and data < date_trunc(current_date("America/Sao_Paulo"), month)
            and {{ incremental_filter }}
    ),
    /*
        6. veiculo_v1
        Dados dos veículos com filtro de tipo e ano de fabricação
        [Entre 2025-01-01 e 2025-03-31]
    */
    veiculo_v1 as (
        select svd.data, svd.id_veiculo, tipo_veiculo, ano_fabricacao, l.placa
        from {{ ref("sppo_veiculo_dia") }} as svd
        left join {{ ref("licenciamento_data_versao_efetiva") }} as ldvf using (data)
        left join
            {{ ref("licenciamento") }} as l
            on l.data = ldvf.data_versao
            and l.id_veiculo = svd.id_veiculo
        where
            svd.data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
            and svd.data >= "{{ var('DATA_SUBSIDIO_V13_INICIO') }}"
    ),
    /*
        7. veiculo_v2
        Dados dos veículos com filtro de tipo e ano de fabricação
        [A partir de 2025-04-01]
    */
    veiculo_v2 as (
        select
            data,
            id_veiculo,
            tipo_veiculo,
            coalesce(vd.ano_fabricacao, vld.ano_fabricacao) as ano_fabricacao,
            coalesce(vd.placa, vld.placa) as placa
        from veiculo_dia as vd
        left join
            veiculo_licenciamento_dia as vld using (
                data, data_processamento, id_veiculo
            )
    ),
    /*
        8. veiculo_raw
        União dos dados dos veículos com filtro de tipo e ano de fabricação (v1 + v2)
    */
    veiculo_raw as (
        select *
        from veiculo_v1
        union all
        select *
        from veiculo_v2
    ),
    /*
        9. veiculo_viagem
        Une os dados de veículos com os dados de viagens completas e aplica os filtros de tipo e ano de fabricação
    */
    veiculo_viagem as (
        select *
        from viagem_completa
        left join veiculo_raw using (data, id_veiculo)
        where
            tipo_veiculo not in (
                '44 BRT PADRON',
                '45 BRT ARTICULADO',
                '46 BRT BIARTICULADO',
                '61 RODOV. C/AR E ELEV',
                '5 ONIBUS ROD. C/ AR'
            )
            and ano_fabricacao is not null
    )
select
    data,
    id_veiculo,
    placa,
    ano_fabricacao,
    ano_fabricacao >= 2023 as indicador_euro_vi,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from veiculo_viagem
