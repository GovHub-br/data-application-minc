-- Silver SALIC — silver_sac_vrankbanda (ex-sac__vrankbanda).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vrankbanda, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 6 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_timestamp("dtprotocolo") }} as dtprotocolo,
    {{ bronze_inteiro("tempodemanda") }} as tempodemanda,
    {{ bronze_texto("pontodemanda") }} as pontodemanda,
    {{ bronze_timestamp("dtfundacao") }} as dtfundacao,
    {{ bronze_inteiro("antiguidade") }} as antiguidade,
    {{ bronze_inteiro("pontoantiguidade") }} as pontoantiguidade,
    {{ bronze_inteiro("finalidade") }} as finalidade,
    {{ bronze_texto("pontofinalidade") }} as pontofinalidade,
    {{ bronze_texto("indicacao") }} as indicacao,
    {{ bronze_texto("pontoindicacao") }} as pontoindicacao,
    {{ bronze_texto("emenda") }} as emenda,
    {{ bronze_texto("pontoemenda") }} as pontoemenda,
    _fatia
from {{ source("salic", "bronze_sac_vrankbanda") }}
