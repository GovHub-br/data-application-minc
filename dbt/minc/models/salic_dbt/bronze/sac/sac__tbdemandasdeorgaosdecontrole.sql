-- Bronze SALIC — sac__tbdemandasdeorgaosdecontrole.
-- Origem: salic_bronze.sac__tbdemandasdeorgaosdecontrole, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 9 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddemandasdeorgaosdecontrole") }} as iddemandasdeorgaosdecontrole,
    {{ bronze_inteiro("cdsolicitante") }} as cdsolicitante,
    {{ bronze_texto("dsdocumentosolicitacao") }} as dsdocumentosolicitacao,
    {{ bronze_timestamp("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_inteiro("nrseisolicitacao") }} as nrseisolicitacao,
    {{ bronze_texto("nrprocesso") }} as nrprocesso,
    {{ bronze_inteiro("sidemanda") }} as sidemanda,
    {{ bronze_texto("dssolicitacao") }} as dssolicitacao,
    {{ bronze_inteiro("nrdiasresponder") }} as nrdiasresponder,
    {{ bronze_timestamp("dtfinalresposta") }} as dtfinalresposta,
    {{ bronze_inteiro("nrseiresposta") }} as nrseiresposta,
    {{ bronze_booleano("stpriorizar") }} as stpriorizar,
    _fatia
from {{ source("bronze_sac", "sac__tbdemandasdeorgaosdecontrole") }}
