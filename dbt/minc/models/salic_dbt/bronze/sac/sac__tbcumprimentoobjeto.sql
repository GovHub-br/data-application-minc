-- Bronze SALIC — sac__tbcumprimentoobjeto.
-- Origem: salic_bronze.sac__tbcumprimentoobjeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 21 colunas: 9 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcumprimentoobjeto") }} as idcumprimentoobjeto,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_timestamp("dtcadastro") }} as dtcadastro,
    {{ bronze_texto("dsetapasconcluidas") }} as dsetapasconcluidas,
    {{ bronze_texto("dsmedidasacessibilidade") }} as dsmedidasacessibilidade,
    {{ bronze_texto("dsmedidasfruicao") }} as dsmedidasfruicao,
    {{ bronze_texto("dsmedidaspreventivas") }} as dsmedidaspreventivas,
    {{ bronze_texto("dsinformacaoadicional") }} as dsinformacaoadicional,
    {{ bronze_texto("dsorientacao") }} as dsorientacao,
    {{ bronze_texto("dsconclusao") }} as dsconclusao,
    {{ bronze_texto("stresultadoavaliacao") }} as stresultadoavaliacao,
    {{ bronze_inteiro("idusuariocadastrador") }} as idusuariocadastrador,
    {{ bronze_inteiro("idtecnicoavaliador") }} as idtecnicoavaliador,
    {{ bronze_texto("sicumprimentoobjeto") }} as sicumprimentoobjeto,
    {{ bronze_inteiro("idchefiaimediata") }} as idchefiaimediata,
    {{ bronze_inteiro("qtempregosdiretos") }} as qtempregosdiretos,
    {{ bronze_inteiro("qtempregosindiretos") }} as qtempregosindiretos,
    {{ bronze_texto("dsgeracaoempregos") }} as dsgeracaoempregos,
    {{ bronze_timestamp("dtenviodaprestacaocontas") }} as dtenviodaprestacaocontas,
    {{ bronze_texto("dsmetadadosavaliacao") }} as dsmetadadosavaliacao,
    _fatia
from {{ source("bronze_sac", "sac__tbcumprimentoobjeto") }}
