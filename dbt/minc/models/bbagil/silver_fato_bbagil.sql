{{ config(enabled=false) }}
-- Silver bbagil — lado-valor VIA BANCO (BB Gestão Ágil / SERPRO): o valor PAGO ao
-- beneficiário final, não o repasse a entes. Lê bronze_fato_bbagil. Grão da
-- fonte: (ente, documento) já agregado e já filtrado pela DAG (só débito p/
-- beneficiário, sem repasse entre entes públicos, ≥R$375).
-- DESABILITADO junto com bronze_fato_bbagil até a extração rodar (ver lá).
--
-- POR QUE ISSO IMPORTA: eixo2_meta3_pagamento_elegivel (lado das listas de
-- contemplados) mede valor RECEBIDO pelos entes (~R$2,7bi ≈ painel "recebido").
-- O BB Ágil mede o valor GASTO/pago às pessoas (~R$447mi painel "gasto"), o
-- denominador correto p/ cotas.
--
-- documento_beneficiario_bbagil vem REAL (não mascarado) → casa por CPF/CNPJ com
-- eixo2_meta3_perfil_agente via a MESMA macro normaliza_documento.
select
    ente_bbagil,
    {{ normaliza_documento('documento_beneficiario_bbagil') }}          as identificador_unico,
    documento_beneficiario_bbagil                                        as documento_raw,
    (documento_beneficiario_bbagil like '%*%')                           as chave_anonimizada,
    {{ parse_valor('valor_transacao_total_bbagil') }}                    as valor_pago_num
from {{ ref('bronze_fato_bbagil') }}
where {{ normaliza_documento('documento_beneficiario_bbagil') }} is not null
   or {{ parse_valor('valor_transacao_total_bbagil') }} is not null
