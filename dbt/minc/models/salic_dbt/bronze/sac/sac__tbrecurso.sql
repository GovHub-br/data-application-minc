-- Bronze SALIC — sac__tbrecurso.
-- Origem: salic_bronze.sac__tbrecurso, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 8 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idrecurso") }} as idrecurso,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_timestamp("dtsolicitacaorecurso") }} as dtsolicitacaorecurso,
    {{ bronze_texto("dssolicitacaorecurso") }} as dssolicitacaorecurso,
    {{ bronze_inteiro("idagentesolicitante") }} as idagentesolicitante,
    {{ bronze_timestamp("dtavaliacao") }} as dtavaliacao,
    {{ bronze_texto("dsavaliacao") }} as dsavaliacao,
    {{ bronze_texto("tprecurso") }} as tprecurso,
    {{ bronze_texto("tpsolicitacao") }} as tpsolicitacao,
    {{ bronze_inteiro("idagenteavaliador") }} as idagenteavaliador,
    {{ bronze_texto("statendimento") }} as statendimento,
    {{ bronze_texto("sifaseprojeto") }} as sifaseprojeto,
    {{ bronze_texto("sirecurso") }} as sirecurso,
    {{ bronze_texto("stanalise") }} as stanalise,
    {{ bronze_inteiro("idnrreuniao") }} as idnrreuniao,
    {{ bronze_booleano("stestado") }} as stestado,
    _fatia
from {{ source("bronze_sac", "sac__tbrecurso") }}
