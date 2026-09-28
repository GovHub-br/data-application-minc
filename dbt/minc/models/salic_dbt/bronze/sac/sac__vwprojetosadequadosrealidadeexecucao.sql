-- Bronze SALIC — sac__vwprojetosadequadosrealidadeexecucao.
-- Origem: salic_bronze.sac__vwprojetosadequadosrealidadeexecucao, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 21 colunas: 12 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto("ufprojeto") }} as ufprojeto,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_inteiro("qtdias") }} as qtdias,
    {{ bronze_inteiro("idtecnico") }} as idtecnico,
    {{ bronze_texto("tecnico") }} as tecnico,
    {{ bronze_texto("enquadramento") }} as enquadramento,
    {{ bronze_numerico("vlsolicitado") }} as vlsolicitado,
    {{ bronze_numerico("vloutrasfontes") }} as vloutrasfontes,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_numerico("vlprojeto") }} as vlprojeto,
    _fatia
from {{ source("bronze_sac", "sac__vwprojetosadequadosrealidadeexecucao") }}
