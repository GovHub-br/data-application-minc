-- Bronze SALIC — sac__tbgerarpagamentoparecerista.
-- Origem: salic_bronze.sac__tbgerarpagamentoparecerista, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 7 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idgerarpagamentoparecerista") }} as idgerarpagamentoparecerista,
    {{ bronze_inteiro("idconfigurarpagamento") }} as idconfigurarpagamento,
    {{ bronze_timestamp("dtgeracaopagamento") }} as dtgeracaopagamento,
    {{ bronze_timestamp("dtefetivacaopagamento") }} as dtefetivacaopagamento,
    {{ bronze_timestamp("dtordembancaria") }} as dtordembancaria,
    {{ bronze_texto("nrordembancaria") }} as nrordembancaria,
    {{ bronze_texto("nrdespacho") }} as nrdespacho,
    {{ bronze_texto("sipagamento") }} as sipagamento,
    {{ bronze_numerico("vltotalpagamento") }} as vltotalpagamento,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbgerarpagamentoparecerista") }}
