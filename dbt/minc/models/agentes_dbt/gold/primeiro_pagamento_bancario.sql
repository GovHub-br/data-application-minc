{{ config(materialized='table') }}

-- Gold — Meta 5 Fase 4: primeiro pagamento real, comprovado pelo extrato
-- bancário do BB Gestão Ágil, por beneficiário × programa de fomento.
-- Aplica o mesmo limiar de valor (R$375, LIMIAR_VALOR_BBAGIL em
-- config_bsc_pnab.py) usado no fato_bbagil original, sobre o total pago
-- por beneficiário+programa — descarta ruído de transações residuais que
-- não representam um repasse de fomento de verdade.
--
-- ATENÇÃO — cobertura temporal: o extrato bancário só existe a partir de
-- 2023-02 (início do BB Ágil como canal de fundo a fundo da LPG/PNAB).
-- "Primeiro pagamento" aqui significa "primeiro pagamento comprovado
-- desde 2023 neste canal", não "primeiro fomento cultural recebido na
-- vida do proponente" — não captura, por exemplo, Lei Aldir Blanc I
-- (2020-2021) nem fomento fora do fundo a fundo federal. Essa é uma
-- limitação estrutural da fonte, não do pipeline.

WITH agregado_programa AS (
    SELECT
        beneficiario_documento,
        programa_fomento,
        MIN(data_pagamento) AS data_primeiro_pagamento,
        SUM(valor) AS valor_total_pago
    FROM {{ ref('pagamentos_bbagil_beneficiarios') }}
    GROUP BY beneficiario_documento, programa_fomento
),

acima_limiar AS (
    SELECT *
    FROM agregado_programa
    WHERE valor_total_pago >= 375
)

SELECT
    beneficiario_documento,
    programa_fomento,
    data_primeiro_pagamento,
    valor_total_pago,
    MIN(data_primeiro_pagamento) OVER (PARTITION BY beneficiario_documento) AS data_primeiro_pagamento_geral,
    CASE
        WHEN data_primeiro_pagamento = MIN(data_primeiro_pagamento) OVER (PARTITION BY beneficiario_documento)
            THEN 'Sim'
        ELSE 'Não'
    END AS categoria_primeiro_acesso_bancario
FROM acima_limiar
