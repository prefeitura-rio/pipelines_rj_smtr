-- depends_on: {{ ref('aux_sppo_licenciamento_vistoria_atualizada') }}
{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

{% if execute and is_incremental() %}
    {% set licenciamento_date = run_query(get_license_date()).columns[0].values()[0] %}
{% endif %}

with
    stu as (
        select * except (data), date(data) as data
        from {{ ref("licenciamento_stu_staging") }} as t
        {% if is_incremental() %}
            where date(data) = date("{{ licenciamento_date }}")
        {% endif %}
    ),
    -- Processo.Rio MTR-CAP-2025/01125 [Correção da alteração do tipo de veículo]
    stu_tipo_veiculo as (
        select
            * except (tipo_veiculo),
            case
                when
                    id_veiculo in (
                        "B27131",
                        "B27009",
                        "B27013",
                        "B27014",
                        "B27019",
                        "B27020",
                        "B27005",
                        "B27075",
                        "B27087",
                        "B27085",
                        "B27084",
                        "B27025",
                        "B27074",
                        "B27070",
                        "B27076",
                        "B27026",
                        "D13132",
                        "D13128",
                        "D13127",
                        "D13126",
                        "D13124",
                        "C47600",
                        "C47577",
                        "C47584",
                        "C47530",
                        "C47543",
                        "C47545",
                        "B31101",
                        "B31103",
                        "B31102",
                        "B31054",
                        "B31055",
                        "B31057",
                        "B31058",
                        "B31066",
                        "B31100",
                        "B31051",
                        "B31099",
                        "B31094",
                        "B31050"
                    )
                    and data
                    between date_add("2025-02-01", interval 5 day) and date_add(
                        "2025-02-14", interval 5 day
                    )
                    and tipo_veiculo = "61 RODOV. C/AR E ELEV"
                then "51 ONIBUS BS URB C/AR C/E 2CAT"
                else tipo_veiculo
            end as tipo_veiculo
        from stu
    ),
    stu_rn as (
        select
            * except (timestamp_captura),
            extract(year from data_ultima_vistoria) as ano_ultima_vistoria,
            row_number() over (partition by data, id_veiculo) rn
        from stu_tipo_veiculo
    ),
    stu_ano_ultima_vistoria as (
        -- Temporariamente considerando os dados de vistoria enviados pela TR/SUBTT/CGLF
        select
            s.* except (ano_ultima_vistoria),
            case
                when
                    data between "2024-03-01" and "2024-12-31"
                    and c.ano_ultima_vistoria > s.ano_ultima_vistoria
                then c.ano_ultima_vistoria
                when data between "2024-03-01" and "2024-12-31"
                then coalesce(s.ano_ultima_vistoria, c.ano_ultima_vistoria)
                else s.ano_ultima_vistoria
            end as ano_ultima_vistoria_atualizado,
        from stu_rn as s
        left join
            (
                select id_veiculo, placa, ano_ultima_vistoria
                from {{ ref("aux_sppo_licenciamento_vistoria_atualizada") }}
            ) as c using (id_veiculo, placa)
    )
select
    data,
    modo,
    id_veiculo,
    ano_fabricacao,
    carroceria,
    data_ultima_vistoria,
    id_carroceria,
    id_chassi,
    id_fabricante_chassi,
    id_interno_carroceria,
    id_planta,
    indicador_ar_condicionado,
    indicador_elevador,
    indicador_usb,
    indicador_wifi,
    nome_chassi,
    permissao,
    placa,
    case
        when tipo_veiculo like "%BASIC%" or tipo_veiculo like "%BS%"
        then "BASICO"
        when tipo_veiculo like "%MIDI%"
        then "MIDI"
        when tipo_veiculo like "%MINI%"
        then "MINI"
        when tipo_veiculo like "%PDRON%" or tipo_veiculo like "%PADRON%"
        then "PADRON"
        when tipo_veiculo like "%ARTICULADO%"
        then "ARTICULADO"
        else null
    end as tecnologia,
    quantidade_lotacao_pe,
    quantidade_lotacao_sentado,
    tipo_combustivel,
    tipo_veiculo,
    status,
    data_inicio_vinculo,
    ano_ultima_vistoria_atualizado,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao
from stu_ano_ultima_vistoria
where rn = 1
