{{
    config(
        alias="operador_stu",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 50000000},
        },
    )
}}

with
    vistoria_dedup as (
        select *
        from {{ ref("staging_stu_vistoria") }}
        qualify row_number() over (partition by ratr order by ano_exercicio desc) = 1
    )
select
    cast(cpf as int64) as cpf_particao,
    nome,
    lpad(cpf, 11, '0') as documento,
    case
        tptran
        when '1'
        then 'Táxi'
        when '2'
        then 'Ônibus'
        when '3'
        then 'Escolar'
        when '4'
        then 'Complementar (cabritinho)'
        when '6'
        then 'Fretamento'
        when '7'
        then 'TEC'
        when '8'
        then 'Van'
        else null
    end as modo,
    case
        tpperm
        when '1'
        then 'Autônomo'
        when '2'
        then 'Empresa'
        when '3'
        then 'Cooperativa'
        when '4'
        then 'Instituição de Ensino'
        when '5'
        then 'Associações'
        when '6'
        then 'Autônomo Provisório'
        when '7'
        then 'Contrato Público'
        when '8'
        then 'Prestadora de Serviços'
        else null
    end as tipo_permissao,
    concat(tptran, tpperm, '.', lpad(termo, 6, '0'), '-', dv) as numero_permissao
from {{ ref("staging_stu_pessoa_fisica") }} as pf
left join vistoria_dedup as v using (ratr)
left join {{ ref("staging_stu_permissao") }} as p using (tptran, tpperm, termo)
