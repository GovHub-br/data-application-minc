-- Silver SALIC — silver_sac_vwpagamentofornecedorirregularreceitafederal (ex-sac__vwpagamentofornecedorirregularreceitafederal).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpagamentofornecedorirregularreceitafederal, onde tudo
-- chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 23 colunas: 11 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("cdproduto") }} as cdproduto,
    {{ bronze_texto("dsproduto") }} as dsproduto,
    {{ bronze_inteiro("cdetapa") }} as cdetapa,
    {{ bronze_texto("dsetapa") }} as dsetapa,
    {{ bronze_inteiro("cduf") }} as cduf,
    {{ bronze_texto("dsuf") }} as dsuf,
    {{ bronze_inteiro("cdmunicipio") }} as cdmunicipio,
    {{ bronze_texto("dsmunicipio") }} as dsmunicipio,
    {{ bronze_inteiro("cditem") }} as cditem,
    {{ bronze_texto("dsitem") }} as dsitem,
    {{ bronze_numerico("vlcomprovacao") }} as vlcomprovacao,
    {{ bronze_texto("nrcnpj") }} as nrcnpj,
    {{ bronze_texto("nmempresarial") }} as nmempresarial,
    {{ bronze_data("dtabertura") }} as dtabertura,
    {{ bronze_data("dtsituacaocadastral") }} as dtsituacaocadastral,
    {{ bronze_timestamp("dtpagamento") }} as dtpagamento,
    {{ bronze_inteiro("cdsituacaocadastral") }} as cdsituacaocadastral,
    {{ bronze_texto("dssituacaocadastral") }} as dssituacaocadastral,
    {{ bronze_texto("situacao") }} as situacao,
    _fatia
from {{ source("salic", "bronze_sac_vwpagamentofornecedorirregularreceitafederal") }}
