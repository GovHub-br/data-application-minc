-- Bronze SALIC — sac__vwprogramarouanetavaliacaoinicialmediasnotasproposta.
-- Origem: salic_bronze.sac__vwprogramarouanetavaliacaoinicialmediasnotasproposta, onde
-- tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("nrproposta") }} as nrproposta,
    {{ bronze_texto("nmproposta") }} as nmproposta,
    {{ bronze_texto("dspergunta") }} as dspergunta,
    {{ bronze_numerico("nrpontuacao") }} as nrpontuacao,
    _fatia
from
    {{
        source(
            "bronze_sac", "sac__vwprogramarouanetavaliacaoinicialmediasnotasproposta"
        )
    }}
