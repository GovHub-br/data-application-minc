-- Silver SALIC — silver_sac_vwpaineldemensagens (ex-sac__vwpaineldemensagens).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwpaineldemensagens, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 25 colunas: 12 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("codarea") }} as codarea,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("codsegmento") }} as codsegmento,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("enquadramento") }} as enquadramento,
    {{ bronze_numerico("vlsolicitado") }} as vlsolicitado,
    {{ bronze_inteiro("idmensagemprojeto") }} as idmensagemprojeto,
    {{ bronze_timestamp("dtmensagem") }} as dtmensagem,
    {{ bronze_inteiro("qtdedias") }} as qtdedias,
    {{ bronze_texto("dsmensagem") }} as dsmensagem,
    {{ bronze_texto("cdtipomensagem") }} as cdtipomensagem,
    {{ bronze_inteiro("idremetente") }} as idremetente,
    {{ bronze_texto("nmremetente") }} as nmremetente,
    {{ bronze_texto("dsunidaderemetente") }} as dsunidaderemetente,
    {{ bronze_inteiro("idremetenteunidade") }} as idremetenteunidade,
    {{ bronze_inteiro("iddestinatario") }} as iddestinatario,
    {{ bronze_texto("nmdestinatario") }} as nmdestinatario,
    {{ bronze_inteiro("iddestinatariounidade") }} as iddestinatariounidade,
    {{ bronze_texto("dsunidadedestinatario") }} as dsunidadedestinatario,
    {{ bronze_inteiro("idmensagemorigem") }} as idmensagemorigem,
    {{ bronze_texto("stativo") }} as stativo,
    _fatia
from {{ source("salic", "bronze_sac_vwpaineldemensagens") }}
