-- Silver SALIC — silver_sac_vwlaudofinalavaliacaoderesultadodoprojeto
-- (ex-sac__vwlaudofinalavaliacaoderesultadodoprojeto).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwlaudofinalavaliacaoderesultadodoprojeto, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 4 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_inteiro("idlaudofinal") }} as idlaudofinal,
    {{ bronze_timestamp("dtlaudofinal") }} as dtlaudofinal,
    {{ bronze_texto("simanifestacao") }} as simanifestacao,
    {{ bronze_texto("dsmanifestacao") }} as dsmanifestacao,
    {{ bronze_texto("dslaudofinal") }} as dslaudofinal,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("salic", "bronze_sac_vwlaudofinalavaliacaoderesultadodoprojeto") }}
