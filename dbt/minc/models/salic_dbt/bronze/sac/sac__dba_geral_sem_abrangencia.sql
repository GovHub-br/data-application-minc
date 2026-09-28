-- Bronze SALIC — sac__dba_geral_sem_abrangencia.
-- Origem: salic_bronze.sac__dba_geral_sem_abrangencia, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 28 colunas: 8 tipadas, 19 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("projeto_pronac") }} as projeto_pronac,
    {{ bronze_texto("projeto_projeto") }} as projeto_projeto,
    {{ bronze_texto("projeto_ano") }} as projeto_ano,
    {{ bronze_inteiro("projeto_sequencial") }} as projeto_sequencial,
    {{ bronze_texto("projeto_uf") }} as projeto_uf,
    {{ bronze_texto("area_descricao") }} as area_descricao,
    {{ bronze_texto("segmento_descricao") }} as segmento_descricao,
    {{ bronze_inteiro("projeto_mecanismo") }} as projeto_mecanismo,
    {{ bronze_texto("projeto_nome") }} as projeto_nome,
    {{ bronze_texto("situacao_descricao") }} as situacao_descricao,
    {{ bronze_texto("proponente_codigo") }} as proponente_codigo,
    {{ bronze_texto("proponente_nome") }} as proponente_nome,
    {{ bronze_texto("proponente_cidade") }} as proponente_cidade,
    {{ bronze_timestamp("projeto_data_inicio_execucao") }}
    as projeto_data_inicio_execucao,
    {{ bronze_timestamp("projeto_data_fim_execucao") }} as projeto_data_fim_execucao,
    {{ bronze_texto("projeto_data_inicio_execucao_formatado") }}
    as projeto_data_inicio_execucao_formatado,
    {{ bronze_texto("projeto_data_fim_execucao_formatado") }}
    as projeto_data_fim_execucao_formatado,
    {{ bronze_numerico("projeto_valor_solicitado_real") }}
    as projeto_valor_solicitado_real,
    {{ bronze_texto("aprovacao_data_inicio_captacao") }}
    as aprovacao_data_inicio_captacao,
    {{ bronze_texto("aprovacao_data_fim_captacao") }} as aprovacao_data_fim_captacao,
    {{ bronze_numerico("aprovacao_valor_aprovado_real") }}
    as aprovacao_valor_aprovado_real,
    {{ bronze_texto("captacao_numero_recibo") }} as captacao_numero_recibo,
    {{ bronze_texto("captacao_tipo_apoio") }} as captacao_tipo_apoio,
    {{ bronze_texto("captacao_data_recibo") }} as captacao_data_recibo,
    {{ bronze_numerico("captacao_valor_captacao_real") }} as captacao_valor_captacao_real,
    {{ bronze_texto("investidor_codigo") }} as investidor_codigo,
    {{ bronze_texto("investidor_nome") }} as investidor_nome,
    _fatia
from {{ source("bronze_sac", "sac__dba_geral_sem_abrangencia") }}
