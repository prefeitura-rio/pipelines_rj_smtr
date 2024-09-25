{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key="id_transacao",
    )
}}

{% set incremental_filter %}
  DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
{% endset %}

with
    staging_transacao as (
        select *
        from `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.transacao_riocard`
        where
            {% if is_incremental() %} {{ incremental_filter }}
            {% else %} date(data) >= "2024-09-13"
            {% endif %}
    ),
    novos_dados as (
        select
            extract(date from t.data_transacao) as data,
            extract(hour from t.data_transacao) as hora,
            t.data_transacao as datetime_transacao,
            t.data_processamento as datetime_processamento,
            t.timestamp_captura as datetime_captura,
            coalesce(do.modo, dc.modo) as modo,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            t.cd_linha as id_servico_jae,
            l.nr_linha as servico_jae,
            l.nm_linha as descricao_servico_jae,
            t.sentido,
            case
                when do.modo = "VLT"
                then substring(t.veiculo_id, 1, 3)
                when do.modo = "BRT"
                then null
                else t.veiculo_id
            end as id_veiculo,
            t.numero_serie_validador as id_validador,
            t.id as id_transacao,
            t.latitude_trx as latitude,
            t.longitude_trx as longitude,
            st_geogpoint(t.longitude_trx, t.latitude_trx) as geo_point_transacao,
            t.valor_transacao
        from staging_transacao t
        left join
            `rj-smtr.cadastro.operadoras` do on t.cd_operadora = do.id_operadora_jae
        left join
            `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.linha` l
            on t.cd_linha = l.cd_linha
        left join
            `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.linha_consorcio` lc
            on t.cd_linha = lc.cd_linha
            and (
                t.data_transacao between lc.dt_inicio_validade and lc.dt_fim_validade
                or lc.dt_fim_validade is null
            )
        left join
            `rj-smtr.cadastro.consorcios` dc on lc.cd_consorcio = dc.id_consorcio_jae
    ),
    transacao_deduplicada as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_transacao order by datetime_captura desc
                    ) as rn
                from novos_dados
            )
        where rn = 1
    )
select *, '{{ var("version") }}' as versao
from transacao_deduplicada
