-- Bronze SALIC — sac__vwpagamentoparecerista.
-- Origem: salic_bronze.sac__vwpagamentoparecerista, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 20 colunas: 12 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idgerarpagamentoparecerista") }} as idgerarpagamentoparecerista,
    {{ bronze_inteiro("nrreuniao") }} as nrreuniao,
    {{ bronze_inteiro("anoefetivacao") }} as anoefetivacao,
    {{ bronze_inteiro("mesefetivacao") }} as mesefetivacao,
    {{ bronze_texto("sigla") }} as sigla,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("parecerista") }} as parecerista,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_inteiro("nrdespacho") }} as nrdespacho,
    {{ bronze_timestamp("dtgeracaopagamento") }} as dtgeracaopagamento,
    {{ bronze_timestamp("dtefetivacaopagamento") }} as dtefetivacaopagamento,
    {{ bronze_timestamp("dtordembancaria") }} as dtordembancaria,
    {{ bronze_texto("nrordembancaria") }} as nrordembancaria,
    {{ bronze_texto("sipagamento") }} as sipagamento,
    {{ bronze_numerico("vlpagamento") }} as vlpagamento,
    _fatia
from {{ source("bronze_sac", "sac__vwpagamentoparecerista") }}
