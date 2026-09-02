-- Bronze SALIC — sac__vwprogramarouanetavaliacaoinicialnotasporproposta.
-- Origem: salic_bronze.sac__vwprogramarouanetavaliacaoinicialnotasporproposta, onde
-- tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_texto("dspergunta") }} as dspergunta,
    {{ bronze_inteiro("nrpontuacao") }} as nrpontuacao,
    {{ bronze_inteiro("idavaliador") }} as idavaliador,
    {{ bronze_texto("siavaliacao") }} as siavaliacao,
    {{ bronze_inteiro("nrordem") }} as nrordem,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanetavaliacaoinicialnotasporproposta") }}
