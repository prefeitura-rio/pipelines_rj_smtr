{{ config(materialized="view") }}

with
    planejada as (select * from {{ ref("frota_planejada") }}),
    realizada as (select * from {{ ref("frota_realizada") }}),
    merged as (
        select
            realizada.data as data,
            realizada.linha as linha,
            realizada.pico as pico,
            frota_operante_media as frota_realizada,
            frota_planejada,
            round(frota_operante_media / frota_planejada, 2) as porcentagem_operacao,
        from realizada
        inner join
            planejada
            on realizada.data = planejada.data
            and realizada.linha = planejada.linha
            and realizada.pico = planejada.pico
    )
select
    data,
    linha,
    pico,
    frota_realizada,
    frota_planejada,
    porcentagem_operacao,
    case when porcentagem_operacao < 0.8 then true else false end as multavel
from merged
