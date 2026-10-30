-- Silver SALIC — silver_sac_vwprogramarouanetavaliacaofinalizada
-- (ex-sac__vwprogramarouanetavaliacaofinalizada).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwprogramarouanetavaliacaofinalizada, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 4 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_texto("nmprograma") }} as nmprograma,
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("dsproduto") }} as dsproduto,
    {{ bronze_texto("nmregiao") }} as nmregiao,
    {{ bronze_texto("dsuf") }} as dsuf,
    {{ bronze_texto("nmmunicipio") }} as nmmunicipio,
    {{ bronze_numerico_cientifico("vlproposta") }} as vlproposta,
    {{ bronze_numerico("nrpontuacao") }} as nrpontuacao,
    {{ bronze_texto("nrpontuacaominima") }} as nrpontuacaominima,
    {{ bronze_texto("stsituacao") }} as stsituacao,
    {{ bronze_inteiro("nrranking") }} as nrranking,
    _fatia
from {{ source("salic", "bronze_sac_vwprogramarouanetavaliacaofinalizada") }}
