-- Bronze SALIC — sac__vwprojetosliberadosparaadequacaorealidadeexecucao.
-- Origem: salic_bronze.sac__vwprojetosliberadosparaadequacaorealidadeexecucao, onde
-- tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 23 colunas: 10 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    {{ bronze_inteiro("qtdias") }} as qtdias,
    {{ bronze_texto("nrportaria") }} as nrportaria,
    {{ bronze_timestamp("dtpublicacao") }} as dtpublicacao,
    {{ bronze_texto("agencia") }} as agencia,
    {{ bronze_texto("contabloqueada") }} as contabloqueada,
    {{ bronze_inteiro("stproposta") }} as stproposta,
    {{ bronze_texto("excecao") }} as excecao,
    {{ bronze_texto("stdatafixa") }} as stdatafixa,
    {{ bronze_texto("datamarcada") }} as datamarcada,
    {{ bronze_texto("tpprorrogacao") }} as tpprorrogacao,
    {{ bronze_texto("solicitacaoprorrogacao") }} as solicitacaoprorrogacao,
    {{ bronze_numerico("vlsolicitado") }} as vlsolicitado,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_numerico("vlcaptado") }} as vlcaptado,
    {{ bronze_numerico("perccaptado") }} as perccaptado,
    _fatia
from {{ source("bronze_sac", "sac__vwprojetosliberadosparaadequacaorealidadeexecucao") }}
