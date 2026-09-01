-- Bronze SALIC — sac__vwanalisefinanceiravirtual.
-- Origem: salic_bronze.sac__vwanalisefinanceiravirtual, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 19 colunas: 6 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("cdarea") }} as cdarea,
    {{ bronze_texto("dsarea") }} as dsarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    {{ bronze_texto("dssegmento") }} as dssegmento,
    {{ bronze_texto("cdsituacao") }} as cdsituacao,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_inteiro("cdorgao") }} as cdorgao,
    {{ bronze_texto("dsmecanismo") }} as dsmecanismo,
    {{ bronze_texto("dssituacaoencprestcontas") }} as dssituacaoencprestcontas,
    {{ bronze_inteiro("idagentedestino") }} as idagentedestino,
    {{ bronze_texto("nmtecnico") }} as nmtecnico,
    {{ bronze_timestamp("dtinicioencaminhamento") }} as dtinicioencaminhamento,
    {{ bronze_texto("dtfimencaminhamento") }} as dtfimencaminhamento,
    {{ bronze_texto("qtdiasemanalise") }} as qtdiasemanalise,
    {{ bronze_texto("idsituacaoencprestcontas") }} as idsituacaoencprestcontas,
    _fatia
from {{ source("bronze_sac", "sac__vwanalisefinanceiravirtual") }}
