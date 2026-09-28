-- Bronze SALIC — sac__editaldesembolso.
-- Origem: salic_bronze.sac__editaldesembolso, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 8 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddesembolso") }} as iddesembolso,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("idedital") }} as idedital,
    {{ bronze_inteiro("nrparcela") }} as nrparcela,
    {{ bronze_timestamp("data") }} as data,
    {{ bronze_numerico("vlcapital") }} as vlcapital,
    {{ bronze_numerico("vlcusteio") }} as vlcusteio,
    {{ bronze_booleano("pagou") }} as pagou,
    {{ bronze_texto("nrempenho") }} as nrempenho,
    {{ bronze_texto("nrordembancaria") }} as nrordembancaria,
    {{ bronze_texto("nrempenhocusteio") }} as nrempenhocusteio,
    {{ bronze_texto("nrordembancariacusteio") }} as nrordembancariacusteio,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__editaldesembolso") }}
