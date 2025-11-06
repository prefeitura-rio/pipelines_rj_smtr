with
    cadastro as (
        select
            id_operadora,
            case
                when modo_stu = "Complementar (cabritinho)"
                then "STPC"
                when modo_stu = "Van"
                then "STPL"
                else modo_stu
            end as modo
        from {{ ref("operadoras") }}
        where modo = "Van"
    ),
    licenciamento_raw as (
        select
            id_operadora, safe_cast(ultimo_ano_vistoria as int64) as ultimo_ano_vistoria
        from `rj-smtr.dashboard_subsidio_van_staging.20241017 _licenciamento_van`
    ),
    licenciamento as (
        select
            id_operadora,
            case
                when id_operadora = "810010844"
                then 2023  -- Correção em razão DO calendário de vistoria
                when id_operadora = "810003408"
                then 2024  -- Permissão com permuta de veículo
                when
                    id_operadora in (
                        '810020465',
                        '463061127',
                        '810012479',
                        '810005626',
                        '760061192',
                        '810014192',
                        '810004298',
                        '810018015',
                        '810013065',
                        '810001536',
                        '760074402',
                        '760015667',
                        '463140088',
                        '810000737',
                        '810010491',
                        '810003994',
                        '810013135',
                        '810010695',
                        '810007534',
                        '810010400',
                        '810014174',
                        '810007145',
                        '810017252',
                        '810014235',
                        '810010002',
                        '810003967',
                        '810003523',
                        '760005019',
                        '810020784',
                        '810006805',
                        '810006179',
                        '810012114',
                        '810007880'
                    )
                then 2022  -- Correções
                else ultimo_ano_vistoria
            end as ultimo_ano_vistoria
        from licenciamento_raw
        where ultimo_ano_vistoria >= 2023
    ),
    gps_raw_0 as (
        select
            data,
            datetime_gps,
            id_operadora,
            id_validador,
            st_geogpoint(longitude, latitude) as posicao_veiculo_geo,
        from {{ ref("gps_validador_van") }}
        where data = "2024-10-06"
    ),
    gps_raw as (
        select
            data,
            datetime_gps,
            id_operadora,
            id_validador,
            st_geogpoint(longitude, latitude) as posicao_veiculo_geo,
            estado_equipamento,
        from {{ ref("gps_validador_van") }}
        where data = "2024-10-06" and (latitude != 0 or longitude != 0)
    ),
    gps_treated as (
        select
            *,
            st_distance(
                posicao_veiculo_geo,
                lag(posicao_veiculo_geo) over (
                    partition by id_operadora, id_validador order by datetime_gps
                )
            ) distancia,
            ifnull(
                safe_divide(
                    st_distance(
                        posicao_veiculo_geo,
                        lag(posicao_veiculo_geo) over (
                            partition by id_operadora, id_validador
                            order by datetime_gps
                        )
                    ),
                    datetime_diff(
                        datetime_gps,
                        lag(datetime_gps) over (
                            partition by id_operadora, id_validador
                            order by datetime_gps
                        ),
                        second
                    )
                ),
                0
            )
            * 3.6 velocidade
        from gps_raw
    ),
    gps_distancia as (
        select data, id_operadora, id_validador, sum(distancia) as distancia_total
        from gps_treated
        where velocidade between 0 and 200
        group by 1, 2, 3
    ),
    gps_contagem as (
        select
            data,
            id_operadora,
            id_validador,
            min(datetime_gps) as inicio_operacao,
            max(datetime_gps) as fim_operacao,
            count(*) as quantidade_gps
        from gps_raw_0
        group by 1, 2, 3
    ),
    tabela_agregada as (
        select
            id_operadora,
            id_validador,
            modo,
            inicio_operacao,
            fim_operacao,
            round(distancia_total / 1000, 2) as km_percorrida,
            case when quantidade_gps > 0 then true else false end as indicador_gps,
            case when distancia_total > 0 then true else false end as indicador_km,
            case
                when ultimo_ano_vistoria >= 2023 then true else false
            end as indicador_vistoria
        from cadastro
        left join licenciamento using (id_operadora)
        left join gps_distancia using (id_operadora)
        left join gps_contagem using (data, id_operadora, id_validador)
    )
select
    *,
    case
        when
            (
                indicador_gps is true
                and indicador_km is true
                and indicador_vistoria is true
            )
        then true
        else false
    end as indicador_subsidio
from tabela_agregada
order by id_operadora, id_validador
