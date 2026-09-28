-- Bronze SALIC — sac__vwpaineldedocumentosaguardandoassinatura.
-- Origem: salic_bronze.sac__vwpaineldedocumentosaguardandoassinatura, onde tudo chega
-- como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 24 colunas: 16 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_inteiro("cdarea") }} as cdarea,
    {{ bronze_texto("resumoprojeto") }} as resumoprojeto,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_inteiro("dias") }} as dias,
    {{ bronze_inteiro("iddocumentoassinatura") }} as iddocumentoassinatura,
    {{ bronze_inteiro("possuiproximaassinatura") }} as possuiproximaassinatura,
    {{ bronze_inteiro("quantidadeassinaturas") }} as quantidadeassinaturas,
    {{ bronze_inteiro("quantidadetotalassinaturas") }} as quantidadetotalassinaturas,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("tp_enquadramento") }} as tp_enquadramento,
    {{ bronze_inteiro("idtipodoatoadministrativo") }} as idtipodoatoadministrativo,
    {{ bronze_texto("tipodoatoadministrativo") }} as tipodoatoadministrativo,
    {{ bronze_inteiro("idordemdaassinatura") }} as idordemdaassinatura,
    {{ bronze_inteiro("idatoadministrativo") }} as idatoadministrativo,
    {{ bronze_inteiro("idperfildoassinante") }} as idperfildoassinante,
    {{ bronze_inteiro("idorgaodoassinante") }} as idorgaodoassinante,
    {{ bronze_inteiro("idorgaosuperiordoassinante") }} as idorgaosuperiordoassinante,
    {{ bronze_inteiro("ordemdaproximaassinatura") }} as ordemdaproximaassinatura,
    _fatia
from {{ source("bronze_sac", "sac__vwpaineldedocumentosaguardandoassinatura") }}
