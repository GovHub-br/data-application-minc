-- Bronze SALIC — sac__dba_geral.
-- Origem: salic_bronze.sac__dba_geral, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 48 colunas: 5 tipadas, 42 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("projeto_pronac") }} as projeto_pronac,
    {{ bronze_texto("projeto_projeto") }} as projeto_projeto,
    {{ bronze_texto("projeto_ano") }} as projeto_ano,
    {{ bronze_texto("projeto_sequencial") }} as projeto_sequencial,
    {{ bronze_texto("projeto_uf") }} as projeto_uf,
    {{ bronze_inteiro("area_codigo") }} as area_codigo,
    {{ bronze_texto("area_descricao") }} as area_descricao,
    {{ bronze_texto("segmento_descricao") }} as segmento_descricao,
    {{ bronze_texto("projeto_mecanismo") }} as projeto_mecanismo,
    {{ bronze_texto("projeto_nome") }} as projeto_nome,
    {{ bronze_texto("projeto_resumo") }} as projeto_resumo,
    {{ bronze_texto("situacao_codigo") }} as situacao_codigo,
    {{ bronze_texto("situacao_descricao") }} as situacao_descricao,
    {{ bronze_texto("proponente_codigo") }} as proponente_codigo,
    {{ bronze_texto("proponente_tipo") }} as proponente_tipo,
    {{ bronze_texto("proponente_nome") }} as proponente_nome,
    {{ bronze_texto("proponente_cidade") }} as proponente_cidade,
    {{ bronze_texto("proponente_uf") }} as proponente_uf,
    {{ bronze_texto("proponente_celular") }} as proponente_celular,
    {{ bronze_texto("proponente_comercial") }} as proponente_comercial,
    {{ bronze_texto("proponente_email") }} as proponente_email,
    {{ bronze_texto("proponente_responsavel") }} as proponente_responsavel,
    {{ bronze_texto("projeto_data_inicio_execucao") }} as projeto_data_inicio_execucao,
    {{ bronze_texto("projeto_data_fim_execucao") }} as projeto_data_fim_execucao,
    {{ bronze_texto("projeto_data_inicio_execucao_formatado") }}
    as projeto_data_inicio_execucao_formatado,
    {{ bronze_texto("projeto_data_fim_execucao_formatado") }}
    as projeto_data_fim_execucao_formatado,
    {{ bronze_numerico("projeto_valor_solicitado_real") }}
    as projeto_valor_solicitado_real,
    {{ bronze_texto("aprovacao_data_aprovacao") }} as aprovacao_data_aprovacao,
    {{ bronze_texto("aprovacao_data_aprovacao_formatado") }}
    as aprovacao_data_aprovacao_formatado,
    {{ bronze_texto("aprovacao_data_inicio_captacao") }}
    as aprovacao_data_inicio_captacao,
    {{ bronze_texto("aprovacao_data_fim_captacao") }} as aprovacao_data_fim_captacao,
    {{ bronze_texto("aprovacao_data_inicio_captacao_formatado") }}
    as aprovacao_data_inicio_captacao_formatado,
    {{ bronze_texto("aprovacao_data_fim_captacao_formatado") }}
    as aprovacao_data_fim_captacao_formatado,
    {{ bronze_texto("aprovacao_valor_aprovado_real") }} as aprovacao_valor_aprovado_real,
    {{ bronze_texto("captacao_numero_recibo") }} as captacao_numero_recibo,
    {{ bronze_texto("captacao_tipo_apoio") }} as captacao_tipo_apoio,
    {{ bronze_timestamp("captacao_data_recibo") }} as captacao_data_recibo,
    {{ bronze_texto("captacao_data_recibo_formatado") }}
    as captacao_data_recibo_formatado,
    {{ bronze_numerico("captacao_valor_captacao_real") }} as captacao_valor_captacao_real,
    {{ bronze_texto("investidor_codigo") }} as investidor_codigo,
    {{ bronze_texto("investidor_tipo") }} as investidor_tipo,
    {{ bronze_texto("investidor_nome") }} as investidor_nome,
    {{ bronze_texto("investidor_cidade") }} as investidor_cidade,
    {{ bronze_texto("investidor_uf") }} as investidor_uf,
    {{ bronze_texto("municipio_abrangencia") }} as municipio_abrangencia,
    {{ bronze_texto("uf_municipio_abrangencia") }} as uf_municipio_abrangencia,
    {{ bronze_texto("convenio_valor") }} as convenio_valor,
    _fatia
from {{ source("bronze_sac", "sac__dba_geral") }}
