-- Bronze SALIC — sac__vwprojetos_na_pauta_cnic.
-- Origem: salic_bronze.sac__vwprojetos_na_pauta_cnic, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 30 colunas: 10 tipadas, 19 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("analise") }} as analise,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_texto("componente") }} as componente,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("resumoprojeto") }} as resumoprojeto,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto("codsituacao") }} as codsituacao,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_timestamp("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_texto("descarea") }} as descarea,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("descsegmento") }} as descsegmento,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_inteiro("dias") }} as dias,
    {{ bronze_texto("idnrreuniao") }} as idnrreuniao,
    {{ bronze_texto("nrreuniao") }} as nrreuniao,
    {{ bronze_texto("stanalise") }} as stanalise,
    {{ bronze_texto("avaliacao") }} as avaliacao,
    {{ bronze_numerico("solicitadoreal") }} as solicitadoreal,
    {{ bronze_texto("sugeridoreal") }} as sugeridoreal,
    {{ bronze_texto("enquadramento") }} as enquadramento,
    {{ bronze_timestamp("dtdistribuicaocomponente") }} as dtdistribuicaocomponente,
    _fatia
from {{ source("bronze_sac", "sac__vwprojetos_na_pauta_cnic") }}
