{{ config(materialized='table') }}

-- Silver — unifica extrato-folha (bbagil_extrato_filtrado, já sem as
-- transações com subtransactionquantity > 0) e subtransações filtradas
-- (bbagil_subtransacoes_filtrado) em 1 linha por pagamento real a um
-- beneficiário. Deriva programa_fomento (LPG/PNAB) a partir do código
-- curto do programa devolvido pelo BSC (governmentprogramname).
--
-- Cobertura: só cobre pagamentos feitos pelos 9 programas atualmente
-- extraídos pela extracao_bbagil_dag (Variable transferegov_programas_ids).
-- Programas fora desse escopo (nem MINC-LPG*, nem MINC-PNAB*/*A BLANC*)
-- caem em programa_fomento NULL e são descartados aqui.

WITH pagamentos AS (
    SELECT id_plano_acao, programa_curto, beneficiario_documento, valor, data_pagamento
    FROM {{ ref('bbagil_extrato_filtrado') }}

    UNION ALL

    SELECT id_plano_acao, programa_curto, beneficiario_documento, valor, data_pagamento
    FROM {{ ref('bbagil_subtransacoes_filtrado') }}
),

classificado AS (
    SELECT
        id_plano_acao,
        beneficiario_documento,
        valor,
        data_pagamento,
        CASE
            WHEN programa_curto LIKE 'MINC-LPG%' THEN 'LPG'
            WHEN programa_curto LIKE 'MINC-PNAB%' THEN 'PNAB'
            WHEN programa_curto LIKE '%A BLANC%' THEN 'PNAB'
            ELSE NULL
        END AS programa_fomento
    FROM pagamentos
)

SELECT
    id_plano_acao,
    beneficiario_documento,
    programa_fomento,
    valor,
    data_pagamento
FROM classificado
WHERE beneficiario_documento IS NOT NULL
  AND data_pagamento IS NOT NULL
  AND programa_fomento IS NOT NULL
