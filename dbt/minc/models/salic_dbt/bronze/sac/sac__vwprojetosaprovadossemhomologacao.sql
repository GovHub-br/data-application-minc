-- Bronze SALIC — sac__vwprojetosaprovadossemhomologacao.
-- Origem: salic_bronze.sac__vwprojetosaprovadossemhomologacao, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 28 colunas: 9 tipadas, 18 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tipopessoa") }} as tipopessoa,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("dssituacao") }} as dssituacao,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    {{ bronze_texto("dsexcecao") }} as dsexcecao,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_texto("resumoprojeto") }} as resumoprojeto,
    {{ bronze_texto("objetivos") }} as objetivos,
    {{ bronze_texto("justificativa") }} as justificativa,
    {{ bronze_texto("democratizacaodeacesso") }} as democratizacaodeacesso,
    {{ bronze_texto("acessibilidade") }} as acessibilidade,
    {{ bronze_texto("idfase") }} as idfase,
    {{ bronze_texto("dsfase") }} as dsfase,
    {{ bronze_texto("tpexcecao") }} as tpexcecao,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_numerico("vladequado") }} as vladequado,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_numerico("percaptado") }} as percaptado,
    _fatia
from {{ source("bronze_sac", "sac__vwprojetosaprovadossemhomologacao") }}
