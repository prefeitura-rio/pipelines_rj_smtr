{{ config(materialized="ephemeral") }} select * from {{ var("limites_caixa") }}
