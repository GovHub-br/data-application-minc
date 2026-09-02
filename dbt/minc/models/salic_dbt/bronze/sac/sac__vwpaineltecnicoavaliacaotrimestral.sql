-- Bronze SALIC — sac__vwpaineltecnicoavaliacaotrimestral.
-- Origem: salic_bronze.sac__vwpaineltecnicoavaliacaotrimestral, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 16 colunas: 10 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("ufprojeto") }} as ufprojeto,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_timestamp("dtcomprovante") }} as dtcomprovante,
    {{ bronze_timestamp("dtinicioperiodo") }} as dtinicioperiodo,
    {{ bronze_timestamp("dtfimperiodo") }} as dtfimperiodo,
    {{ bronze_texto("sicomprovantetrimestral") }} as sicomprovantetrimestral,
    {{ bronze_inteiro("nrcomprovantetrimestral") }} as nrcomprovantetrimestral,
    {{ bronze_texto("diligencia") }} as diligencia,
    {{ bronze_inteiro("idtecnicoavaliador") }} as idtecnicoavaliador,
    _fatia
from {{ source("bronze_sac", "sac__vwpaineltecnicoavaliacaotrimestral") }}
