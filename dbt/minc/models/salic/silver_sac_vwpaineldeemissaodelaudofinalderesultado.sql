-- Silver SALIC — silver_sac_vwpaineldeemissaodelaudofinalderesultado
-- (ex-sac__vwpaineldeemissaodelaudofinalderesultado).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpaineldeemissaodelaudofinalderesultado, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 2 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_inteiro("cdorgao") }} as cdorgao,
    {{ bronze_texto("dsresutadodaavaliacaodoobjeto") }} as dsresutadodaavaliacaodoobjeto,
    {{ bronze_texto("dsresutadodaavaliacaofinanceira") }}
    as dsresutadodaavaliacaofinanceira,
    _fatia
from {{ source("salic", "bronze_sac_vwpaineldeemissaodelaudofinalderesultado") }}
