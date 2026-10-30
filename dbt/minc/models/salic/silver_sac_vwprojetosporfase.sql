-- Silver SALIC — silver_sac_vwprojetosporfase (ex-sac__vwprojetosporfase).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwprojetosporfase, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 8 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_texto("sigla") }} as sigla,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_timestamp("dttransformacao") }} as dttransformacao,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_texto("stexcecao") }} as stexcecao,
    {{ bronze_texto("stfluxo") }} as stfluxo,
    {{ bronze_texto("fase") }} as fase,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    _fatia
from {{ source("salic", "bronze_sac_vwprojetosporfase") }}
