-- Bronze SALIC — sac__vwpaineldehomologacaodeprojetos.
-- Origem: salic_bronze.sac__vwpaineldehomologacaodeprojetos, onde tudo chega como texto
-- da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 10 colunas: 6 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("enquadramento") }} as enquadramento,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_inteiro("nrreuniao") }} as nrreuniao,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_numerico("vlhomologado") }} as vlhomologado,
    _fatia
from {{ source("bronze_sac", "sac__vwpaineldehomologacaodeprojetos") }}
