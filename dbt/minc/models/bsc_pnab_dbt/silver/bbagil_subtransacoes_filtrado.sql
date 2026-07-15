{{ config(materialized='table') }}

-- Traducao 1:1 dos 5 filtros de pipeline_filtro_subtransacoes
-- (plugins/regras_negocio_bbagil.py), na mesma ordem.

WITH base AS (
    SELECT *
    FROM {{ ref('bbagil_subtransacoes') }}
),

-- 1. Remove sem beneficiario valido
beneficiario_valido AS (
    SELECT *
    FROM base
    WHERE beneficiarydocumentid <> '0'
),

-- 2. Mantem apenas pagas
apenas_pagas AS (
    SELECT *
    FROM beneficiario_valido
    WHERE subtransactionaccountabilityname = 'Pago'
),

-- 3. Normaliza para valor absoluto + 4. corte temporal (Ciclo 1 do PNAB)
normalizado AS (
    SELECT
        *,
        ABS(value) AS valor_absoluto
    FROM apenas_pagas
    WHERE paymentdate <= '{{ var("bsc_pnab_data_corte") }}'::date
),

-- 5. Remove repasses entre entes publicos
sem_repasse_publico AS (
    SELECT *
    FROM normalizado
    WHERE beneficiaryname IS NULL
       OR NOT (
            UPPER(beneficiaryname) LIKE 'MUNICIPIO%'
            OR UPPER(beneficiaryname) LIKE 'ESTADO%'
            OR UPPER(beneficiaryname) LIKE 'FUNDO%'
            OR UPPER(beneficiaryname) LIKE 'SECRETARIA%'
            OR UPPER(beneficiaryname) LIKE 'SEFAZ%'
       )
)

SELECT
    ente,
    beneficiarydocumentid,
    valor_absoluto AS valor_pago
FROM sem_repasse_publico
