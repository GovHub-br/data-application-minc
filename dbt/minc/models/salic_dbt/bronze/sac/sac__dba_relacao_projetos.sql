-- Bronze SALIC — sac__dba_relacao_projetos.
-- Origem: salic_bronze.sac__dba_relacao_projetos, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 6 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("projeto_nome") }} as projeto_nome,
    {{ bronze_texto("proponente_codigo") }} as proponente_codigo,
    {{ bronze_texto("proponente_nome") }} as proponente_nome,
    {{ bronze_texto("proponente_cidade") }} as proponente_cidade,
    {{ bronze_texto("mecanismo") }} as mecanismo,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("enquadramento") }} as enquadramento,
    {{ bronze_inteiro("ano_apresentacao") }} as ano_apresentacao,
    {{ bronze_inteiro("mes_apresentacao") }} as mes_apresentacao,
    {{ bronze_numerico("valor_aprovado") }} as valor_aprovado,
    {{ bronze_numerico("valor_apoiado") }} as valor_apoiado,
    {{ bronze_texto("situacao_codigo") }} as situacao_codigo,
    {{ bronze_texto("situacao_descricao") }} as situacao_descricao,
    {{ bronze_inteiro("ano_situacao") }} as ano_situacao,
    {{ bronze_inteiro("mes_situacao") }} as mes_situacao,
    {{ bronze_texto("cidade_execucao") }} as cidade_execucao,
    _fatia
from {{ source("bronze_sac", "sac__dba_relacao_projetos") }}
