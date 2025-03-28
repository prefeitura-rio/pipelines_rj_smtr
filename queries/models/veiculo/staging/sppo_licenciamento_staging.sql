{{ config(materialized="table", alias="sppo_licenciamento") }}
with
    datetime_atualizacao as (
        select
            parse_date('%Y%m%d', partition_id) as data,
            datetime(
                last_modified_time, "America/Sao_Paulo"
            ) as datetime_ultima_atualizacao,
        from `rj-smtr.veiculo.INFORMATION_SCHEMA.PARTITIONS`
        where table_name = 'sppo_licenciamento' and partition_id != "__NULL__"
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
    safe_cast(null as string) as tecnologia,
    quantidade_lotacao_pe,
    quantidade_lotacao_sentado,
    tipo_combustivel,
    tipo_veiculo,
    status,
    data_inicio_vinculo,
    ano_ultima_vistoria_atualizado,
    datetime_ultima_atualizacao,
    safe_cast(null as string) as versao
from `rj-smtr.veiculo.sppo_licenciamento`
left join datetime_atualizacao using (data)
where data <= "2024-12-31"
