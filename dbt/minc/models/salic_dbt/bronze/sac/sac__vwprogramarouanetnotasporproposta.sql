-- Bronze SALIC — sac__vwprogramarouanetnotasporproposta.
-- Origem: salic_bronze.sac__vwprogramarouanetnotasporproposta, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 4 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_texto("dspergunta") }} as dspergunta,
    {{ bronze_numerico("nrpontuacao") }} as nrpontuacao,
    {{ bronze_numerico("qtpontosrecurso") }} as qtpontosrecurso,
    {{ bronze_texto("siavaliacaorecurso") }} as siavaliacaorecurso,
    {{ bronze_inteiro("nrordem") }} as nrordem,
    _fatia
from {{ source("bronze_sac", "sac__vwprogramarouanetnotasporproposta") }}
