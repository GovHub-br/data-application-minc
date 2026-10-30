-- Silver SALIC — silver_sac_distribuicaopassagem (ex-sac__distribuicaopassagem).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_distribuicaopassagem, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 6 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("rt") }} as rt,
    {{ bronze_texto("fatura") }} as fatura,
    {{ bronze_numerico("vlconcedido") }} as vlconcedido,
    {{ bronze_booleano("administrativa") }} as administrativa,
    {{ bronze_timestamp("dtentregabilhete") }} as dtentregabilhete,
    {{ bronze_timestamp("dtdevolucaobilhete") }} as dtdevolucaobilhete,
    {{ bronze_timestamp("dtrelatorio") }} as dtrelatorio,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("salic", "bronze_sac_distribuicaopassagem") }}
