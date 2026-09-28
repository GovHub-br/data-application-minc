-- Bronze SALIC — sac__voutrasinformacoesaudio.
-- Origem: salic_bronze.sac__voutrasinformacoesaudio, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 3 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("produtor") }} as produtor,
    {{ bronze_texto("diretor") }} as diretor,
    {{ bronze_texto("roteirista") }} as roteirista,
    {{ bronze_texto("metragem") }} as metragem,
    {{ bronze_texto("genero") }} as genero,
    {{ bronze_texto("veiculacao") }} as veiculacao,
    {{ bronze_texto("suportegravacao") }} as suportegravacao,
    {{ bronze_texto("finalizacao") }} as finalizacao,
    {{ bronze_texto("duracaotipo") }} as duracaotipo,
    {{ bronze_inteiro("duracaoqtde") }} as duracaoqtde,
    {{ bronze_inteiro("duracaocada") }} as duracaocada,
    {{ bronze_inteiro("duracaototal") }} as duracaototal,
    _fatia
from {{ source("bronze_sac", "sac__voutrasinformacoesaudio") }}
