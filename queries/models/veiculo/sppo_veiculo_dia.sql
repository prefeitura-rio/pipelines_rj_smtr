{# -- depends_on: {{ ref('licenciamento_stu_staging') }}
-- depends_on: {{ ref("infracao") }}
-- depends_on: {{ ref("infracao_staging") }} #}
{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

{% if execute %}
    {% set licenciamento_date = run_query(get_license_date()).columns[0].values()[0] %}
    {% set infracao_date = run_query(get_violation_date()).columns[0].values()[0] %}
{% endif %}

with
    licenciamento as (
        select
            date("{{ var('run_date') }}") as data,
            id_veiculo,
            placa,
            tipo_veiculo,
            tecnologia,
            indicador_ar_condicionado,
            true as indicador_licenciado,
            case
                when
                    ano_ultima_vistoria_atualizado >= cast(
                        extract(
                            year
                            from
                                date_sub(
                                    date("{{ var('run_date') }}"),
                                    interval {{
                                        var(
                                            "sppo_licenciamento_validade_vistoria_ano"
                                        )
                                    }} year
                                )
                        ) as int64
                    )
                then true  -- Última vistoria realizada dentro do período válido
                when
                    data_ultima_vistoria is null
                    and date_diff(
                        date("{{ var('run_date') }}"), data_inicio_vinculo, day
                    )
                    <= {{ var("sppo_licenciamento_tolerancia_primeira_vistoria_dia") }}
                then true  -- Caso o veículo seja novo, existe a tolerância de 15 dias para a primeira vistoria
                when
                    ano_fabricacao in (2023, 2024)
                    and cast(extract(year from date("{{ var('run_date') }}")) as int64)
                    = 2024
                then true  -- Caso o veículo tiver ano de fabricação 2023 ou 2024, será considerado como vistoriado apenas em 2024 (regra de transição)
                else false
            end as indicador_vistoriado,
            data_inicio_vinculo
        from {{ ref("sppo_licenciamento") }}
        where data = date("{{ licenciamento_date }}")
    ),
    gps as (
        select distinct data, id_veiculo
        {# from {{ ref("gps_sppo") }} #}
        from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
        where data = date("{{ var('run_date') }}")
    ),
    autuacoes as (
        select distinct data_infracao as data, placa, id_infracao
        from {{ ref("sppo_infracao") }}
        where
        {# {% if execute %}
            {% set infracao_date = run_query("SELECT MIN(data) FROM rj-smtr.veiculo.infracao WHERE data >= DATE_ADD(DATE('2024-10-15'), INTERVAL 7 DAY)").columns[0].values()[0] %}
        {% endif %} #}
            data = date("{{ infracao_date }}")
            and data_infracao = date("{{ var('run_date') }}")
    ),
    registros_agente_verao as (
        select distinct
            data, id_veiculo, true as indicador_registro_agente_verao_ar_condicionado
        {# from {{ ref("sppo_registro_agente_verao") }} #}
        from `rj-smtr.veiculo.sppo_registro_agente_verao`
        where data = date("{{ var('run_date') }}")
    ),
    autuacao_ar_condicionado as (
        select data, placa, true as indicador_autuacao_ar_condicionado
        from autuacoes
        where id_infracao = "023.II"
    ),
    autuacao_seguranca as (
        select data, placa, true as indicador_autuacao_seguranca
        from autuacoes
        where
            id_infracao in (
                "016.VI",
                "023.VII",
                "024.II",
                "024.III",
                "024.IV",
                "024.V",
                "024.VI",
                "024.VII",
                "024.VIII",
                "024.IX",
                "024.XII",
                "024.XIV",
                "024.XV",
                "025.II",
                "025.XII",
                "025.XIII",
                "025.XIV",
                "026.X"
            )
    ),
    autuacao_equipamento as (
        select data, placa, true as indicador_autuacao_equipamento
        from autuacoes
        where
            id_infracao in (
                "023.IV",
                "023.V",
                "023.VI",
                "023.VIII",
                "024.XIII",
                "024.XI",
                "024.XVIII",
                "024.XXI",
                "025.III",
                "025.IV",
                "025.V",
                "025.VI",
                "025.VII",
                "025.VIII",
                "025.IX",
                "025.X",
                "025.XI"
            )
    ),
    autuacao_limpeza as (
        select data, placa, true as indicador_autuacao_limpeza
        from autuacoes
        where id_infracao in ("023.IX", "024.X")
    ),
    autuacoes_agg as (
        select distinct *
        from autuacao_ar_condicionado
        full join autuacao_seguranca using (data, placa)
        full join autuacao_equipamento using (data, placa)
        full join autuacao_limpeza using (data, placa)
    ),
    gps_licenciamento_autuacao as (
        select
            data,
            id_veiculo,
            {% if var("run_date") >= var("DATA_SUBSIDIO_V5_INICIO") %}
                struct(
                    coalesce(l.indicador_licenciado, false) as indicador_licenciado,
                    coalesce(l.indicador_vistoriado, false) as indicador_vistoriado,
                    coalesce(
                        l.indicador_ar_condicionado, false
                    ) as indicador_ar_condicionado,
                    coalesce(
                        a.indicador_autuacao_ar_condicionado, false
                    ) as indicador_autuacao_ar_condicionado,
                    coalesce(
                        a.indicador_autuacao_seguranca, false
                    ) as indicador_autuacao_seguranca,
                    coalesce(
                        a.indicador_autuacao_limpeza, false
                    ) as indicador_autuacao_limpeza,
                    coalesce(
                        a.indicador_autuacao_equipamento, false
                    ) as indicador_autuacao_equipamento,
                    coalesce(
                        r.indicador_registro_agente_verao_ar_condicionado, false
                    ) as indicador_registro_agente_verao_ar_condicionado
                )
            -- WHEN data >= DATE("DATA_SUBSIDIO_V4_INICIO") THEN
            -- STRUCT( COALESCE(l.indicador_licenciado, FALSE)                     AS
            -- indicador_licenciado,
            -- COALESCE(l.indicador_ar_condicionado, FALSE)                AS
            -- indicador_ar_condicionado,
            -- COALESCE(a.indicador_autuacao_ar_condicionado, FALSE)       AS
            -- indicador_autuacao_ar_condicionado,
            -- COALESCE(a.indicador_autuacao_seguranca, FALSE)             AS
            -- indicador_autuacao_seguranca,
            -- COALESCE(a.indicador_autuacao_limpeza, FALSE)               AS
            -- indicador_autuacao_limpeza,
            -- COALESCE(a.indicador_autuacao_equipamento, FALSE)           AS
            -- indicador_autuacao_equipamento,
            -- COALESCE(r.indicador_registro_agente_verao_ar_condicionado, FALSE)   AS
            -- indicador_registro_agente_verao_ar_condicionado)
            -- WHEN data >= DATE("DATA_SUBSIDIO_V3_INICIO") THEN
            -- STRUCT( COALESCE(l.indicador_licenciado, FALSE)                     AS
            -- indicador_licenciado,
            -- COALESCE(l.indicador_ar_condicionado, FALSE)                AS
            -- indicador_ar_condicionado,
            -- COALESCE(a.indicador_autuacao_ar_condicionado, FALSE)       AS
            -- indicador_autuacao_ar_condicionado,
            -- COALESCE(a.indicador_autuacao_seguranca, FALSE)             AS
            -- indicador_autuacao_seguranca,
            -- COALESCE(a.indicador_autuacao_limpeza, FALSE)               AS
            -- indicador_autuacao_limpeza,
            -- COALESCE(a.indicador_autuacao_equipamento, FALSE)           AS
            -- indicador_autuacao_equipamento)
            -- WHEN data >= DATE("DATA_SUBSIDIO_V2_INICIO") THEN
            -- STRUCT( COALESCE(l.indicador_licenciado, FALSE)                     AS
            -- indicador_licenciado,
            -- COALESCE(l.indicador_ar_condicionado, FALSE)                AS
            -- indicador_ar_condicionado,
            -- COALESCE(a.indicador_autuacao_ar_condicionado, FALSE)       AS
            -- indicador_autuacao_ar_condicionado)
            -- ELSE
            -- NULL
            {% else %}
                struct(
                    coalesce(l.indicador_licenciado, false) as indicador_licenciado,
                    coalesce(
                        l.indicador_ar_condicionado, false
                    ) as indicador_ar_condicionado,
                    coalesce(
                        a.indicador_autuacao_ar_condicionado, false
                    ) as indicador_autuacao_ar_condicionado,
                    coalesce(
                        a.indicador_autuacao_seguranca, false
                    ) as indicador_autuacao_seguranca,
                    coalesce(
                        a.indicador_autuacao_limpeza, false
                    ) as indicador_autuacao_limpeza,
                    coalesce(
                        a.indicador_autuacao_equipamento, false
                    ) as indicador_autuacao_equipamento,
                    coalesce(
                        r.indicador_registro_agente_verao_ar_condicionado, false
                    ) as indicador_registro_agente_verao_ar_condicionado
                )
            {% endif %} as indicadores,
            l.placa,
            tecnologia
        from gps g
        left join licenciamento as l using (data, id_veiculo)
        left join autuacoes_agg as a using (data, placa)
        left join registros_agente_verao as r using (data, id_veiculo)
    )
{% if var("run_date") < var("DATA_SUBSIDIO_V5_INICIO") %}
    select
        gla.* except (indicadores, tecnologia, placa),
        to_json(indicadores) as indicadores,
        status,
        {% if var("run_date") >= var("DATA_SUBSIDIO_V13_INICIO") %}
            tecnologia,
            placa,
            date("{{ licenciamento_date }}") as data_licenciamento,
            date("{{ infracao_date }}") as data_infracao,
        {% else %}
            safe_cast(null as string) as tecnologia,
            safe_cast(null as string) as placa,
            safe_cast(null as date) as data_licenciamento,
            safe_cast(null as date) as data_infracao,
        {% endif %}
        current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
        "{{ var('version') }}" as versao
    from gps_licenciamento_autuacao as gla
    left join
        {# {{ ref("subsidio_parametros") }} as p #}
         `rj-smtr.dashboard_subsidio_sppo.subsidio_parametros` AS p
        on gla.indicadores.indicador_licenciado = p.indicador_licenciado
        and gla.indicadores.indicador_ar_condicionado = p.indicador_ar_condicionado
        and gla.indicadores.indicador_autuacao_ar_condicionado
        = p.indicador_autuacao_ar_condicionado
        and gla.indicadores.indicador_autuacao_seguranca
        = p.indicador_autuacao_seguranca
        and gla.indicadores.indicador_autuacao_limpeza = p.indicador_autuacao_limpeza
        and gla.indicadores.indicador_autuacao_equipamento
        = p.indicador_autuacao_equipamento
        and gla.indicadores.indicador_registro_agente_verao_ar_condicionado
        = p.indicador_registro_agente_verao_ar_condicionado
        and (data between p.data_inicio and p.data_fim)
{% else %}
    select
        * except (indicadores, tecnologia, placa),
        to_json(indicadores) as indicadores,
        case
            when indicadores.indicador_licenciado is false
            then "Não licenciado"
            when indicadores.indicador_vistoriado is false
            then "Não vistoriado"
            when
                indicadores.indicador_ar_condicionado is true
                and indicadores.indicador_autuacao_ar_condicionado is true
            then "Autuado por ar inoperante"
            when
                indicadores.indicador_ar_condicionado is true
                and indicadores.indicador_registro_agente_verao_ar_condicionado is true
            then "Registrado com ar inoperante"
            when indicadores.indicador_autuacao_seguranca is true
            then "Autuado por segurança"
            when
                indicadores.indicador_autuacao_limpeza is true
                and indicadores.indicador_autuacao_equipamento is true
            then "Autuado por limpeza/equipamento"
            when indicadores.indicador_ar_condicionado is false
            then "Licenciado sem ar e não autuado"
            when indicadores.indicador_ar_condicionado is true
            then "Licenciado com ar e não autuado"
            else null
        end as status,
        {% if var("run_date") >= var("DATA_SUBSIDIO_V13_INICIO") %}
            tecnologia,
            placa,
            date("{{ licenciamento_date }}") as data_licenciamento,
            date("{{ infracao_date }}") as data_infracao,
        {% else %}
            safe_cast(null as string) as tecnologia,
            safe_cast(null as string) as placa,
            safe_cast(null as date) as data_licenciamento,
            safe_cast(null as date) as data_infracao,
        {% endif %}
        current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
        "{{ var('version') }}" as versao
    from gps_licenciamento_autuacao
{% endif %}
