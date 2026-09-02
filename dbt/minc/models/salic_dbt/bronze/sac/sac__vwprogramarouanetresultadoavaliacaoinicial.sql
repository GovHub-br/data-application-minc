-- Bronze SALIC — sac__vwprogramarouanetresultadoavaliacaoinicial.
-- Origem: salic_bronze.sac__vwprogramarouanetresultadoavaliacaoinicial, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 3 tipadas, 10 mantidas como texto.
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
    {{ bronze_numerico("nrpontuacao") }} as nrpontuacao,
    {{ bronze_texto("nrpontuacaominima") }} as nrpontuacaominima,
    {{ bronze_texto("vlproposta") }} as vlproposta,
    {{ bronze_inteiro("nrranking") }} as nrranking,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanetresultadoavaliacaoinicial") }}
