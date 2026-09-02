-- Bronze SALIC — sac__vdiligencias.
-- Origem: salic_bronze.sac__vdiligencias, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 4 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("controladores") }} as controladores,
    {{ bronze_inteiro("documentosolicitacao") }} as documentosolicitacao,
    {{ bronze_texto("nrdocumentosolicitacao") }} as nrdocumentosolicitacao,
    {{ bronze_texto("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_inteiro("posicionamento") }} as posicionamento,
    {{ bronze_texto("constatacao") }} as constatacao,
    {{ bronze_texto("documentoresposta") }} as documentoresposta,
    {{ bronze_texto("nrdocumentoresposta") }} as nrdocumentoresposta,
    {{ bronze_texto("dtresposta") }} as dtresposta,
    {{ bronze_texto("resposta") }} as resposta,
    _fatia
from {{ source("bronze_sac", "sac__vdiligencias") }}
