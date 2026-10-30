-- Silver SALIC — silver_sac_vwprogramarouanetdistribuirpropostarecurso (ex-sac__vwprogramarouanetdistribuirpropostarecurso).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwprogramarouanetdistribuirpropostarecurso, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 6 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idunicolinha") }} as idunicolinha,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("tptipologia") }} as tptipologia,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("dsproduto") }} as dsproduto,
    {{ bronze_texto("nmregiao") }} as nmregiao,
    {{ bronze_texto("dsuf") }} as dsuf,
    {{ bronze_texto("nmmunicipio") }} as nmmunicipio,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("nmavaliador") }} as nmavaliador,
    {{ bronze_texto("tpavaliacao") }} as tpavaliacao,
    {{ bronze_texto("sianalise") }} as sianalise,
    {{ bronze_inteiro("usu_codigo") }} as usu_codigo,
    _fatia
from {{ source("salic", "bronze_sac_vwprogramarouanetdistribuirpropostarecurso") }}
