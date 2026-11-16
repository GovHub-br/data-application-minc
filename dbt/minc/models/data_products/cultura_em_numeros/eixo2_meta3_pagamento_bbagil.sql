{{ config(materialized="view") }}

-- Gold — Meta 3 (cotas da LPG e da PNAB): o valor PAGO ao beneficiário final, pelo
-- extrato das contas dos planos de ação no BB Ágil. É o lado-valor da medida das
-- cotas pelo pago, que corre ao lado da medida pelas listas de contemplados
-- (eixo2_meta3_pagamento_elegivel) e não a substitui: trocar o denominador
-- oficial é decisão de regra, para um PR próprio.
--
-- Lê eixo2_meta5_evento_fomento e não refaz a regra de pagamento efetivo: o que
-- conta como pagamento (só débito, sem os dois lados de um estorno, sublançamento
-- 'Pago'), quem conta como agente (fora ente público, FNC, Banco do Brasil e
-- lançamento interno), a política pelo código do programa e o piso de
-- {{ var("limiar_valor_fomento", 375) }} reais sobre o total do agente na política
-- moram lá, uma vez só para as duas metas (Gold lê Gold do mesmo produto, ADR 0015).
--
-- A regra própria da Meta 3 é o escopo: só LPG e PNAB, as duas leis que exigem as
-- cotas. A LAB (Aldir Blanc II) fica de fora.
--
-- Grão: o de eixo2_meta5_evento_fomento, 1 linha por (documento, política, data,
-- valor). O ano é o do pagamento, não o do edital. View: é só um filtro.
select
    beneficiario_documento, programa_fomento, data_evento, ano_evento, valor, tipo_pessoa
from {{ ref("eixo2_meta5_evento_fomento") }}
where fonte = 'BB_AGIL' and programa_fomento in ('LPG', 'PNAB')
