{{ config(materialized='table') }}

-- Camada Gold — Meta 5 Fase 4: indicador final de "novos entrantes"
-- (Lei Paulo Gustavo + PNAB), comprovado só por dado bancário real do
-- BB Ágil — sem depender de resposta autodeclarada em formulário nem de
-- inferência. Responde à pergunta da Meta 5: qual o percentual de agentes
-- culturais contemplados que nunca haviam recebido recursos públicos de
-- fomento antes do pagamento atual?
--
-- Escopo deliberadamente simples: contemplado sem pagamento encontrado no
-- BB Ágil fica de fora do indicador (não cai em nenhum fallback
-- autodeclarado/inferido). Motivo: a base autodeclarada
-- (perfil_acesso_fomento, Fase 1-2) tem um mapeamento Sim/Não cujo
-- sentido não foi possível confirmar contra o texto real do formulário —
-- em vez de arriscar inverter "novo entrante"/"veterano", preferimos
-- cobertura parcial (só quem tem prova bancária) a um número
-- potencialmente errado. Ver primeiro_pagamento_bancario.sql pra
-- cobertura temporal do dado bancário (só a partir de 2023).

WITH todos_contemplados AS (
    SELECT id_normalizado, programa_fomento
    FROM {{ ref('identificadores_contemplados') }}
),

bancario_contemplado AS (
    SELECT
        pb.beneficiario_documento,
        pb.programa_fomento,
        pb.categoria_primeiro_acesso_bancario
    FROM {{ ref('primeiro_pagamento_bancario') }} pb
    INNER JOIN todos_contemplados tc
        ON pb.beneficiario_documento = tc.id_normalizado
        AND pb.programa_fomento = tc.programa_fomento
)

SELECT
    programa_fomento,
    categoria_primeiro_acesso_bancario AS categoria_primeiro_acesso,
    COUNT(DISTINCT beneficiario_documento) AS total_proponentes,
    ROUND(
        COUNT(DISTINCT beneficiario_documento)::NUMERIC
        / SUM(COUNT(DISTINCT beneficiario_documento)) OVER (PARTITION BY programa_fomento)
        * 100, 2
    ) AS percentual
FROM bancario_contemplado
GROUP BY programa_fomento, categoria_primeiro_acesso_bancario
ORDER BY programa_fomento, total_proponentes DESC
