{{ config(materialized="ephemeral") }}

with
    t_velocidade as (
        select
            data,
            id_veiculo,
            datetime_gps,
            servico,
            st_distance(
                posicao_veiculo_geo,
                lag(posicao_veiculo_geo) over (
                    partition by id_veiculo, servico order by datetime_gps
                )
            ) distancia,
            ifnull(
                safe_divide(
                    st_distance(
                        posicao_veiculo_geo,
                        lag(posicao_veiculo_geo) over (
                            partition by id_veiculo, servico order by datetime_gps
                        )
                    ),
                    datetime_diff(
                        datetime_gps,
                        lag(datetime_gps) over (
                            partition by id_veiculo, servico order by datetime_gps
                        ),
                        second
                    )
                ),
                0
            )
            * 3.6 velocidade
        from {{ ref("aux_gps_filtrada") }}
        {% if not flags.FULL_REFRESH -%}
            where
                data between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and datetime_gps > "{{var('date_range_start')}}"
                and datetime_gps <= "{{var('date_range_end')}}"
        {%- endif -%}
    ),
    medias as (
        select
            data,
            id_veiculo,
            datetime_gps,
            servico,
            distancia,
            velocidade,
            avg(velocidade) over (
                partition by id_veiculo, servico
                order by
                    unix_seconds(timestamp(datetime_gps))
                    range
                    between {{ var("janela_movel_velocidade") }} preceding
                    and current row
            ) velocidade_media  -- velocidade com média móvel
        from t_velocidade
    )
select
    data,
    datetime_gps,
    id_veiculo,
    servico,
    distancia,
    round(
        case
            when velocidade_media > {{ var("velocidade_maxima") }}
            then {{ var("velocidade_maxima") }}
            else velocidade_media
        end,
        1
    ) as velocidade,
    case
        when velocidade_media < {{ var("velocidade_limiar_parado") }}
        then false
        else true
    end indicador_em_movimento
from medias
