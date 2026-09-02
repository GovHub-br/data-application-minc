-- Bronze SALIC — sac__tbfiscalizacao.
-- Origem: salic_bronze.sac__tbfiscalizacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 8 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idfiscalizacao") }} as idfiscalizacao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_timestamp("dtiniciofiscalizacaoprojeto") }} as dtiniciofiscalizacaoprojeto,
    {{ bronze_timestamp("dtfimfiscalizacaoprojeto") }} as dtfimfiscalizacaoprojeto,
    {{ bronze_timestamp("dtrespostasolicitada") }} as dtrespostasolicitada,
    {{ bronze_texto("dsfiscalizacaoprojeto") }} as dsfiscalizacaoprojeto,
    {{ bronze_texto("tpdemandante") }} as tpdemandante,
    {{ bronze_texto("stfiscalizacaoprojeto") }} as stfiscalizacaoprojeto,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("idsolicitante") }} as idsolicitante,
    {{ bronze_inteiro("idusuariointerno") }} as idusuariointerno,
    _fatia
from {{ source("bronze_sac", "sac__tbfiscalizacao") }}
