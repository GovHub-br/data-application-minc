-- Bronze SALIC — sac__vwprogramarouanetpropostasrecebidas.
-- Origem: salic_bronze.sac__vwprogramarouanetpropostasrecebidas, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 24 colunas: 12 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetxproposta") }} as idprogramarouanetxproposta,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("nrcnpjcpf") }} as nrcnpjcpf,
    {{ bronze_texto("nmproponente") }} as nmproponente,
    {{ bronze_timestamp("dtiniciodeexecucao") }} as dtiniciodeexecucao,
    {{ bronze_timestamp("dtfinaldeexecucao") }} as dtfinaldeexecucao,
    {{ bronze_inteiro("tptipologia") }} as tptipologia,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_inteiro("cdarea") }} as cdarea,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_texto("dsproduto") }} as dsproduto,
    {{ bronze_texto("nmregiao") }} as nmregiao,
    {{ bronze_inteiro("cduf") }} as cduf,
    {{ bronze_texto("dsuf") }} as dsuf,
    {{ bronze_inteiro("cdcidade") }} as cdcidade,
    {{ bronze_texto("nmmunicipio") }} as nmmunicipio,
    {{ bronze_numerico("vlproposta") }} as vlproposta,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanetpropostasrecebidas") }}
