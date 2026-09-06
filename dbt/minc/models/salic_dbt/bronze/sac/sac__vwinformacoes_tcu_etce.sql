-- Bronze SALIC — sac__vwinformacoes_tcu_etce.
-- Origem: salic_bronze.sac__vwinformacoes_tcu_etce, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 26 colunas: 8 tipadas, 17 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("nun_instrumento") }} as nun_instrumento,
    {{ bronze_inteiro("ano_instrumento") }} as ano_instrumento,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("num_siafi_instrumento") }} as num_siafi_instrumento,
    {{ bronze_texto("num_siconv_instrumento") }} as num_siconv_instrumento,
    {{ bronze_texto("descr_objeto_instrumento") }} as descr_objeto_instrumento,
    {{ bronze_texto("tipo_instrumento") }} as tipo_instrumento,
    {{ bronze_texto("se_mandatario_cef") }} as se_mandatario_cef,
    {{ bronze_texto("programa_projeto") }} as programa_projeto,
    {{ bronze_texto("num_nota_empenho") }} as num_nota_empenho,
    {{ bronze_texto("num_ob") }} as num_ob,
    {{ bronze_texto("num_banco_conta_especifica") }} as num_banco_conta_especifica,
    {{ bronze_texto("num_ag_conta_especifica") }} as num_ag_conta_especifica,
    {{ bronze_texto("num_conta_especifica") }} as num_conta_especifica,
    {{ bronze_texto("uf_destino_recurso") }} as uf_destino_recurso,
    {{ bronze_texto("cnpj_destinatario") }} as cnpj_destinatario,
    {{ bronze_texto("nome_destinatario") }} as nome_destinatario,
    {{ bronze_timestamp("dt_inicio_vigencia") }} as dt_inicio_vigencia,
    {{ bronze_timestamp("dt_fim_vigencia_original") }} as dt_fim_vigencia_original,
    {{ bronze_timestamp("dt_fim_vigencia_final") }} as dt_fim_vigencia_final,
    {{ bronze_inteiro("qtd_prorrogacoes_vigencia") }} as qtd_prorrogacoes_vigencia,
    {{ bronze_texto("situacao_execucao_objeto") }} as situacao_execucao_objeto,
    {{ bronze_numerico("valor_total_instrumento") }} as valor_total_instrumento,
    {{ bronze_numerico("valor_total_transferido") }} as valor_total_transferido,
    {{ bronze_texto("valor_contrapartida") }} as valor_contrapartida,
    _fatia
from {{ source("bronze_sac", "sac__vwinformacoes_tcu_etce") }}
