-- Bronze SALIC — sac__vwprogramarouanetpropostasnaoavaliadas.
-- Origem: salic_bronze.sac__vwprogramarouanetpropostasnaoavaliadas, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 3 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_texto("tptipologia") }} as tptipologia,
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
    {{ bronze_inteiro("usu_codigo") }} as usu_codigo,
    {{ bronze_texto("nrpontos") }} as nrpontos,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanetpropostasnaoavaliadas") }}
