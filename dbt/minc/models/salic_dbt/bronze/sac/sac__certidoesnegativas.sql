-- Bronze SALIC — sac__certidoesnegativas.
-- Origem: salic_bronze.sac__certidoesnegativas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 6 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_inteiro("codigocertidao") }} as codigocertidao,
    {{ bronze_timestamp("dtemissao") }} as dtemissao,
    {{ bronze_timestamp("dtvalidade") }} as dtvalidade,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_inteiro("idcertidoesnegativas") }} as idcertidoesnegativas,
    {{ bronze_texto("cdprotocolonegativa") }} as cdprotocolonegativa,
    {{ bronze_inteiro("cdsituacaocertidao") }} as cdsituacaocertidao,
    _fatia
from {{ source("bronze_sac", "sac__certidoesnegativas") }}
