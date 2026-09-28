-- Bronze SALIC — sac__tbhistoricoalteracaoprojeto.
-- Origem: salic_bronze.sac__tbhistoricoalteracaoprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 17 colunas: 9 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idhistoricoalteracaoprojeto") }} as idhistoricoalteracaoprojeto,
    {{ bronze_texto("cdarea") }} as cdarea,
    {{ bronze_texto("cdsegmento") }} as cdsegmento,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_texto("cdsituacao") }} as cdsituacao,
    {{ bronze_inteiro("cdorgao") }} as cdorgao,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_inteiro("idlogon") }} as idlogon,
    {{ bronze_inteiro("iddocumento") }} as iddocumento,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idenquadramento") }} as idenquadramento,
    {{ bronze_timestamp("dthistoricoalteracaoprojeto") }} as dthistoricoalteracaoprojeto,
    {{ bronze_texto("dshistoricoalteracaoprojeto") }} as dshistoricoalteracaoprojeto,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("dsprovidenciatomada") }} as dsprovidenciatomada,
    _fatia
from {{ source("bronze_sac", "sac__tbhistoricoalteracaoprojeto") }}
