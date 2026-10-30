-- Silver SALIC — silver_sac_vwprogramarouanetpainelsolicitarrecurso (ex-sac__vwprogramarouanetpainelsolicitarrecurso).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwprogramarouanetpainelsolicitarrecurso, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 6 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_inteiro("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_timestamp("dtinicioprazorecursal") }} as dtinicioprazorecursal,
    {{ bronze_timestamp("dtfimprazorecursal") }} as dtfimprazorecursal,
    {{ bronze_numerico("nrpontuacao") }} as nrpontuacao,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("salic", "bronze_sac_vwprogramarouanetpainelsolicitarrecurso") }}
