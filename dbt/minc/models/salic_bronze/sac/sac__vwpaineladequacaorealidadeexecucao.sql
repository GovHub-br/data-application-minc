-- Bronze SALIC — sac__vwpaineladequacaorealidadeexecucao.
-- Origem: salic_bronze.sac__vwpaineladequacaorealidadeexecucao, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 24 colunas: 11 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto("ufprojeto") }} as ufprojeto,
    {{ bronze_texto("dstipologia") }} as dstipologia,
    {{ bronze_texto("dsplanoexecucaoimediata") }} as dsplanoexecucaoimediata,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_texto("orgao") }} as orgao,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_inteiro("qtdias") }} as qtdias,
    {{ bronze_inteiro("idtecnico") }} as idtecnico,
    {{ bronze_texto("tecnico") }} as tecnico,
    {{ bronze_texto("enquadramento") }} as enquadramento,
    {{ bronze_inteiro("qtdiligencias") }} as qtdiligencias,
    {{ bronze_numerico("vlautorizado") }} as vlautorizado,
    {{ bronze_numerico("vladequado") }} as vladequado,
    {{ bronze_numerico("vlpercentualcaptado") }} as vlpercentualcaptado,
    _fatia
from {{ source("bronze_sac", "sac__vwpaineladequacaorealidadeexecucao") }}
