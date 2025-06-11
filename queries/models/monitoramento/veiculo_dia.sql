{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

with
    licenciamento as (
        select
            *,
            case
                when
                    ano_ultima_vistoria >= cast(
                        extract(
                            year
                            from
                                date_sub(
                                    date(data),
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
                    and date_diff(date(data), data_inicio_vinculo, day)
                    <= {{ var("sppo_licenciamento_tolerancia_primeira_vistoria_dia") }}
                then true  -- Caso o veículo seja novo, existe a tolerância de 15 dias para a primeira vistoria
                else false
            end as indicador_vistoriado,
        from {{ ref("veiculo_licenciamento_dia") }}
        where
            data_processamento <= date_add(data, interval 7 day)
            {% if is_incremental() %}
                and data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

            {% endif %}
        qualify
            row_number() over (
                partition by data, id_veiculo, placa order by data_processamento desc
            )
            = 1
    ),
    autuacao_disciplinar as (
        select *
        from {{ ref("autuacao_disciplinar_historico") }}
        where
            data_inclusao <= date_add(data, interval 7 day)
            {% if is_incremental() %}
                and data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

            {% endif %}
    ),
    gps as (
        select *
        from
            {{ ref("aux_veiculo_gps_dia") }}
            {% if is_incremental() %}
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

            {% endif %}
    ),
    registros_agente_verao as (
        select distinct data, id_veiculo
        {# from {{ ref("sppo_registro_agente_verao") }} #}
        from `rj-smtr.veiculo.sppo_registro_agente_verao`
        {% if is_incremental() %}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        {% endif %}
    ),
    gps_licenciamento as (
        select
            data,
            id_veiculo,
            l.placa,
            l.modo,
            l.tecnologia,
            l.id_veiculo is not null as indicador_licenciado,
            l.indicador_vistoriado,
            l.indicador_ar_condicionado,
            r.id_veiculo is not null as indicador_registro_agente_verao_ar_condicionado,
            l.data_processamento as data_processamento_licenciamento,
            r.data as data_registro_agente_verao
        from gps g
        left join licenciamento as l using (data, id_veiculo)
        left join registros_agente_verao as r using (data, id_veiculo)
    ),
    autuacao_ar_condicionado as (
        select data, placa, data_inclusao as data_inclusao_autuacao_ar_condicionado
        from autuacao_disciplinar
        where id_infracao = "023.II"
    ),
    autuacao_seguranca as (
        select data, placa, data_inclusao as data_inclusao_autuacao_seguranca
        from autuacao_disciplinar
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
        select data, placa, data_inclusao as data_inclusao_autuacao_equipamento
        from autuacao_disciplinar
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
        select data, placa, data_inclusao as data_inclusao_autuacao_limpeza
        from autuacao_disciplinar
        where id_infracao in ("023.IX", "024.X")
    ),
    autuacao_completa as (
        select distinct
            data,
            placa,
            ac.data is not null as indicador_autuacao_ar_condicionado,
            s.data is not null as indicador_autuacao_seguranca,
            e.data is not null as indicador_autuacao_equipamento,
            l.data is not null as indicador_autuacao_limpeza,
            ac.data_inclusao_autuacao_ar_condicionado,
            s.data_inclusao_autuacao_seguranca,
            e.data_inclusao_autuacao_equipamento,
            l.data_inclusao_autuacao_limpeza
        from autuacao_ar_condicionado ac
        full outer join autuacao_seguranca s using (data, placa)
        full outer join autuacao_equipamento e using (data, placa)
        full outer join autuacao_limpeza l using (data, placa)
    ),
    gps_licenciamento_autuacao as (
        select
            data,
            gl.id_veiculo,
            placa,
            gl.modo,
            gl.tecnologia,
            struct(
                struct(
                    gl.indicador_licenciado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_licenciado,
                struct(
                    gl.indicador_vistoriado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_vistoriado,
                struct(
                    gl.indicador_ar_condicionado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_ar_condicionado,
                struct(
                    a.indicador_autuacao_ar_condicionado as valor,
                    a.data_inclusao_autuacao_ar_condicionado as data_inclusao_autuacao
                ) as indicador_autuacao_ar_condicionado,
                struct(
                    a.indicador_autuacao_seguranca as valor,
                    a.data_inclusao_autuacao_seguranca as data_inclusao_autuacao
                ) as indicador_autuacao_seguranca,
                struct(
                    a.indicador_autuacao_equipamento as valor,
                    a.data_inclusao_autuacao_equipamento as data_inclusao_autuacao
                ) as indicador_autuacao_equipamento,
                struct(
                    a.indicador_autuacao_limpeza as valor,
                    a.data_inclusao_autuacao_limpeza as data_inclusao_autuacao
                ) as indicador_autuacao_limpeza,
                struct(
                    gl.indicador_registro_agente_verao_ar_condicionado as valor,
                    gl.data_registro_agente_verao as data_registro_agente_verao

                ) as indicador_registro_agente_verao_ar_condicionado
            ) as indicadores
        from gps_licenciamento gl
        left join autuacao_completa as a using (data, placa)
    )
select
    data,
    id_veiculo,
    placa,
    modo,
    tecnologia,
    case
        when indicadores.indicador_licenciado.valor is false
        then "Não licenciado"
        when indicadores.indicador_vistoriado.valor is false
        then "Não vistoriado"
        when
            indicadores.indicador_ar_condicionado.valor is true
            and indicadores.indicador_autuacao_ar_condicionado.valor is true
        then "Autuado por ar inoperante"
        when
            indicadores.indicador_autuacao_ar_condicionado.valor is true
            and indicadores.indicador_registro_agente_verao_ar_condicionado.valor
            is true
        then "Registrado com ar inoperante"
        {# when data < date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
        then
            case
                when indicadores.indicador_autuacao_seguranca.valor is true
                then "Autuado por segurança"
                when
                    indicadores.indicador_autuacao_limpeza.valor is true
                    and indicadores.indicador_autuacao_equipamento.valor is true
                then "Autuado por limpeza/equipamento"
            end #}
        when indicadores.indicador_ar_condicionado.valor is false
        then "Licenciado sem ar e não autuado"
        when indicadores.indicador_ar_condicionado.valor is true
        then "Licenciado com ar e não autuado"
    end as status,
    indicadores,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao
from gps_licenciamento_autuacao
