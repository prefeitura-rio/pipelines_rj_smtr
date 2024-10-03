
{{ config(
        materialized='view'
  )
}}


SELECT
  DATE(data) AS data,
  auinf_num_auto AS id_auto_infracao,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REGEXP_REPLACE(SAFE_CAST(JSON_VALUE(content, '$.auinf_dt_infracao') AS STRING), r'\.\d+', '')), 'America/Sao_Paulo') AS datetime_autuacao,
  IF(JSON_VALUE(content, '$.auinf_dt_limite_defesa_previa') != '', SAFE_CAST(PARSE_DATE('%Y-%m-%d', JSON_VALUE(content,'$.auinf_dt_limite_defesa_previa')) AS STRING), NULL) AS data_limite_defesa_previa,
  IF(JSON_VALUE(content, '$.auinf_dt_limite_recurso') != '', SAFE_CAST(PARSE_DATE('%Y-%m-%d', JSON_VALUE(content,'$.auinf_dt_limite_recurso')) AS STRING), NULL) AS data_limite_recurso,
  SAFE_CAST(JSON_VALUE(content,'$.stat_dsc_status_ai') AS STRING) AS descricao_situacao_autuacao,
  SAFE_CAST(JSON_VALUE(content,'$.stfu_dsc_status_fluxo_ai') AS STRING) AS status_infracao,
  SAFE_CAST(JSON_VALUE(content,'$.htpi_cod_tipo_infracao') AS STRING) AS codigo_enquadramento,
  SAFE_CAST(JSON_VALUE(content,'$.htpi_dsc_tipo_infracao') AS STRING) AS tipificacao_resumida,
  SAFE_CAST(JSON_VALUE(content,'$.htpi_pontosdainfracao') AS STRING) AS pontuacao,
  SAFE_CAST(JSON_VALUE(content,'$.hgrav_descricao') AS STRING) AS gravidade,
  SAFE_CAST(JSON_VALUE(content,'$.htpi_amparo_legal') AS STRING) AS amparo_legal,
  SAFE_CAST(JSON_VALUE(content,'$.auinf_vei_tipo') AS STRING) AS tipo_veiculo,
  SAFE_CAST(JSON_VALUE(content,'$.auinf_veiculo_marca_modelo_informado') AS STRING) AS descricao_veiculo,
  SAFE_CAST(JSON_VALUE(content,'$.auinf_veiculo_placa') AS STRING) AS placa_veiculo,
  SAFE_CAST(JSON_VALUE(content,'$.auinf_veiculo_ano_fabricacao') AS STRING) AS ano_fabricacao_veiculo,
  SAFE_CAST(JSON_VALUE(content,'$.auinf_veiculo_ano_modelo') AS STRING) AS ano_modelo_veiculo,
  SAFE_CAST(JSON_VALUE(content,'$.auinf_veiculo_cor_desc') AS STRING) AS cor_veiculo,
  SAFE_CAST(JSON_VALUE(content,'$.auinf_veiculo_especie_desc') AS STRING) AS especie_veiculo,
  SAFE_CAST(JSON_VALUE(content,'$.uf_infrator') AS STRING) AS uf_infrator,
  SAFE_CAST(JSON_VALUE(content,'$.uf_princ_cond') AS STRING) AS uf_principal_condutor,
  SAFE_CAST(JSON_VALUE(content,'$.uf_prop_orig') AS STRING) AS uf_proprietario,
  SAFE_CAST(JSON_VALUE(content,'$.auinf_infracao_valor') AS NUMERIC) AS valor_infracao,
  SAFE_CAST(JSON_VALUE(content,'$.pag_valor') AS NUMERIC) AS valor_pago,
  SAFE_CAST(JSON_VALUE(content,'$.auinf_id_orgao') AS STRING) AS id_autuador,
  SAFE_CAST(JSON_VALUE(content,'$.unaut_dsc_unidade') AS STRING) AS descricao_autuador,
  SAFE_CAST(JSON_VALUE(content,'$.id_municipio') AS STRING) AS id_municipio_autuacao,
  SAFE_CAST(JSON_VALUE(content,'$.mun_nome') AS STRING) AS descricao_municipio,
  SAFE_CAST(JSON_VALUE(content,'$.uf_sigla') AS STRING) AS uf_autuacao,
  SAFE_CAST(JSON_VALUE(content,'$.defp_num_processo') AS STRING) AS processo_defesa_autuacao,
  SAFE_CAST(JSON_VALUE(content,'$.canc_num_processo') AS STRING) AS recurso_penalidade_multa,
  SAFE_CAST(JSON_VALUE(content,'$.ri_proc_nr') AS STRING) AS processo_troca_real_infrator,
  SAFE_CAST(JSON_VALUE(content,'$.auinf_veiculo_adesao_sne_indicador') AS STRING) AS status_sne
FROM
  {{ source('infracao_staging','autuacoes_serpro') }}

