-- Bronze SALIC — sac__projetos.
-- Origem: salic_bronze.sac__projetos, onde tudo chega como texto da ingestão
-- via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 34 colunas de negócio + _fatia: identificadores e documentos ficam TEXT de
-- propósito (zero à esquerda importa), datas e valores são convertidos por
-- regex — valor fora do padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("ufprojeto") }} as ufprojeto,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_inteiro("segmento") }} as segmento,
    {{ bronze_inteiro("mecanismo") }} as mecanismo,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("processo") }} as processo,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_timestamp("dtprotocolo") }} as dtprotocolo,
    {{ bronze_timestamp("dtanalise") }} as dtanalise,
    {{ bronze_inteiro("modalidade") }} as modalidade,
    {{ bronze_inteiro("orgaoorigem") }} as orgaoorigem,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_timestamp("dtsaida") }} as dtsaida,
    {{ bronze_timestamp("dtretorno") }} as dtretorno,
    {{ bronze_texto("unidadeanalise") }} as unidadeanalise,
    {{ bronze_texto("analista") }} as analista,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_texto("resumoprojeto") }} as resumoprojeto,
    {{ bronze_texto("providenciatomada") }} as providenciatomada,
    {{ bronze_texto("localizacao") }} as localizacao,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_numerico("solicitadoufir") }} as solicitadoufir,
    {{ bronze_numerico("solicitadoreal") }} as solicitadoreal,
    {{ bronze_numerico("solicitadocusteioufir") }} as solicitadocusteioufir,
    {{ bronze_numerico("solicitadocusteioreal") }} as solicitadocusteioreal,
    {{ bronze_numerico("solicitadocapitalufir") }} as solicitadocapitalufir,
    {{ bronze_numerico("solicitadocapitalreal") }} as solicitadocapitalreal,
    {{ bronze_inteiro("logon") }} as logon,
    {{ bronze_texto("idprojeto") }} as idprojeto,
    _fatia
from {{ source("bronze_sac", "sac__projetos") }}
