-- Bronze SALIC — sac__dba_relacao_projetos_fred.
-- Origem: salic_bronze.sac__dba_relacao_projetos_fred, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 32 colunas: 7 tipadas, 24 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("ano_projeto") }} as ano_projeto,
    {{ bronze_texto("projeto_nome") }} as projeto_nome,
    {{ bronze_texto("proponente_codigo") }} as proponente_codigo,
    {{ bronze_texto("proponente_nome") }} as proponente_nome,
    {{ bronze_texto("proponente_cidade") }} as proponente_cidade,
    {{ bronze_texto("mecanismo") }} as mecanismo,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("tipo_apoio") }} as tipo_apoio,
    {{ bronze_texto("enquadramento") }} as enquadramento,
    {{ bronze_texto("secretaria") }} as secretaria,
    {{ bronze_texto("medida_prov") }} as medida_prov,
    {{ bronze_texto("ano_apresentacao") }} as ano_apresentacao,
    {{ bronze_inteiro("mes_apresentacao") }} as mes_apresentacao,
    {{ bronze_texto("data_aprovacao") }} as data_aprovacao,
    {{ bronze_numerico("valor_solicitado_real") }} as valor_solicitado_real,
    {{ bronze_numerico("valor_aprovado") }} as valor_aprovado,
    {{ bronze_numerico("valor_apoiado") }} as valor_apoiado,
    {{ bronze_numerico("valor_captado") }} as valor_captado,
    {{ bronze_texto("captacao_numero_recibo") }} as captacao_numero_recibo,
    {{ bronze_texto("investidor_codigo") }} as investidor_codigo,
    {{ bronze_texto("investidor_tipo") }} as investidor_tipo,
    {{ bronze_texto("investidor_nome") }} as investidor_nome,
    {{ bronze_texto("investidor_cidade") }} as investidor_cidade,
    {{ bronze_texto("investidor_uf") }} as investidor_uf,
    {{ bronze_texto("situacao_codigo") }} as situacao_codigo,
    {{ bronze_texto("situacao_descricao") }} as situacao_descricao,
    {{ bronze_inteiro("ano_situacao") }} as ano_situacao,
    {{ bronze_inteiro("mes_situacao") }} as mes_situacao,
    {{ bronze_texto("cidade_execucao") }} as cidade_execucao,
    _fatia
from {{ source("bronze_sac", "sac__dba_relacao_projetos_fred") }}
