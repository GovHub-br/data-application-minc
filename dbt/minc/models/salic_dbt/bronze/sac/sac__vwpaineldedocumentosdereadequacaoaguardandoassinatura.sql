-- Bronze SALIC — sac__vwpaineldedocumentosdereadequacaoaguardandoassinatura.
-- Origem: salic_bronze.sac__vwpaineldedocumentosdereadequacaoaguardandoassinatura, onde
-- tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 14 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("dstipodoatoadministrativo") }} as dstipodoatoadministrativo,
    {{ bronze_texto("dsreadequacao") }} as dsreadequacao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_inteiro("iddocumentoassinatura") }} as iddocumentoassinatura,
    {{ bronze_inteiro("idtipodoatoadministrativo") }} as idtipodoatoadministrativo,
    {{ bronze_inteiro("qtdedepessoasquefaltamassinar") }}
    as qtdedepessoasquefaltamassinar,
    {{ bronze_inteiro("qtdedepessoasqueassinaramdocumento") }}
    as qtdedepessoasqueassinaramdocumento,
    {{ bronze_inteiro("qtdeassinaturasporatoadministrativo") }}
    as qtdeassinaturasporatoadministrativo,
    {{ bronze_inteiro("idordemdaassinatura") }} as idordemdaassinatura,
    {{ bronze_inteiro("idatoadministrativo") }} as idatoadministrativo,
    {{ bronze_inteiro("idperfildoassinante") }} as idperfildoassinante,
    {{ bronze_inteiro("idorgaodoassinante") }} as idorgaodoassinante,
    {{ bronze_inteiro("idorgaosuperiordoassinante") }} as idorgaosuperiordoassinante,
    {{ bronze_inteiro("ordemdaproximaassinatura") }} as ordemdaproximaassinatura,
    _fatia
from
    {{
        source(
            "bronze_sac", "sac__vwpaineldedocumentosdereadequacaoaguardandoassinatura"
        )
    }}
