-- Silver SALIC — silver_sac_vwpainelcoordenadoravaliacaotrimestral (ex-sac__vwpainelcoordenadoravaliacaotrimestral).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpainelcoordenadoravaliacaotrimestral, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 9 tipadas, 7 mantidas como texto.
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
    {{ bronze_texto("idtecnicoavaliador") }} as idtecnicoavaliador,
    {{ bronze_texto("dsparecertecnico") }} as dsparecertecnico,
    _fatia
from {{ source("salic", "bronze_sac_vwpainelcoordenadoravaliacaotrimestral") }}
