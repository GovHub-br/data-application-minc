-- Silver SALIC — silver_sac_vwpaineldehomologacaodeprojetos (ex-sac__vwpaineldehomologacaodeprojetos).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpaineldehomologacaodeprojetos, onde tudo chega como texto
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
from {{ source("salic", "bronze_sac_vwpaineldehomologacaodeprojetos") }}
