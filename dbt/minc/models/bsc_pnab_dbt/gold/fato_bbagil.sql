{{ config(materialized='table') }}

-- Traducao SQL de montar_fato_bbagil (plugins/regras_negocio_bbagil.py):
-- uniao do extrato filtrado + subtransacoes filtradas, agrupado por
-- (ente, beneficiario), somando o valor pago, com o limiar minimo aplicado.

WITH extrato AS (
    SELECT ente, beneficiarydocumentid, valor_pago
    FROM {{ ref('bbagil_extrato_filtrado') }}
),

subtransacoes AS (
    SELECT ente, beneficiarydocumentid, valor_pago
    FROM {{ ref('bbagil_subtransacoes_filtrado') }}
),

uniao AS (
    SELECT * FROM extrato
    UNION ALL
    SELECT * FROM subtransacoes
),

agregado AS (
    SELECT
        ente AS ente_bbagil,
        beneficiarydocumentid AS documento_beneficiario_bbagil,
        SUM(valor_pago) AS valor_transacao_total_bbagil
    FROM uniao
    GROUP BY ente, beneficiarydocumentid
)

SELECT *
FROM agregado
WHERE valor_transacao_total_bbagil >= {{ var("bsc_pnab_limiar_valor_bbagil") }}
