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
        select
            date(data) as data,
            modo,
            id_veiculo,
            safe_cast(ano_fabricacao as integer) as ano_fabricacao,
            carroceria,
            date(data_ultima_vistoria) as data_ultima_vistoria,
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
            quantidade_lotacao_pe,
            quantidade_lotacao_sentado,
            tipo_combustivel,
            tipo_veiculo,
            status,
            data_inicio_vinculo
        from {{ ref("licenciamento_stu_staging") }} as t
        {% if is_incremental() %}
            where date(data) = date("{{ licenciamento_date }}")
        {% endif %}
    ),
    {# {% if "2022-03-21" <= var('run_date') <= "2023-03-09" %} #}
    solicitacao as (
        select
            modo,
            id_veiculo,
            safe_cast(ano_fabricacao as integer) as ano_fabricacao,
            carroceria,
            date(data_ultima_vistoria) as data_ultima_vistoria,
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
            quantidade_lotacao_pe,
            quantidade_lotacao_sentado,
            tipo_combustivel,
            tipo_veiculo,
            status,
            safe_cast(null as date) as data_inicio_vinculo
        from {{ ref("sppo_licenciamento_solicitacao") }} as t
        where
            data = date("{{ var('sppo_licenciamento_solicitacao_data_versao') }}")  -- fixo 
            {# mudar? #}
            and status = "Válido"
            and solicitacao != "Baixa"
            and tipo_veiculo not like "%ROD%"
            and data between date("2022-03-21") and date("2023-03-09")
    ),
    stu_solicitacoes as (
        select date_add(date("{{ var('run_date') }}"), interval 5 day) as data, *
        from solicitacao sol
        union all
        -- Se tiver id_veiculo em solicitacao e for valido, substitui o que esta em
        -- licenciamento
        select stu.*
        from stu
        left join solicitacao sol on stu.id_veiculo = sol.id_veiculo
        where sol.id_veiculo is null
    ),
    stu_rn as (
        select
            *,
            extract(year from data_ultima_vistoria) as ano_ultima_vistoria,
            row_number() over (partition by data, id_veiculo) rn
        from stu_solicitacoes
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
