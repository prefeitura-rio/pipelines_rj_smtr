{% test test_check_data_arquivo_licenciamento(model) %}
    with
        veiculo_licenciamento_dados as (
            select data, data_arquivo_fonte, id_veiculo, placa
            from {{ model }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        ),
        staging_licenciamento_stu as (
            select date(data) as data, id_veiculo, placa
            from {{ ref("staging_licenciamento_stu") }}
            where
                date(data) between date_sub(
                    date("{{ var('date_range_start') }}"), interval 1 day
                ) and date("{{ var('date_range_end') }}")
        ),
        data_arquivo_mais_recente_por_veiculo as (
            select
                vld.data,
                vld.id_veiculo,
                vld.placa,
                max(sls.data) as data_arquivo_mais_recente
            from veiculo_licenciamento_dados vld
            left join
                staging_licenciamento_stu sls
                on vld.id_veiculo = sls.id_veiculo
                and vld.placa = sls.placa
                and sls.data <= vld.data
            group by vld.data, vld.id_veiculo, vld.placa
        )
    select
        vld.data,
        vld.data_arquivo_fonte,
        damrpv.data_arquivo_mais_recente,
        vld.id_veiculo,
        vld.placa
    from veiculo_licenciamento_dados vld
    join
        data_arquivo_mais_recente_por_veiculo damrpv
        on vld.data = damrpv.data
        and vld.id_veiculo = damrpv.id_veiculo
        and vld.placa = damrpv.placa
    where
        vld.data_arquivo_fonte != damrpv.data_arquivo_mais_recente
        or damrpv.data_arquivo_mais_recente is null
{% endtest %}
