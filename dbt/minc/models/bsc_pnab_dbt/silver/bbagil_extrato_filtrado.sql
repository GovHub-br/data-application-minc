{{ config(materialized='table') }}

-- Traducao 1:1 dos 8 filtros de pipeline_filtro_extrato
-- (plugins/regras_negocio_bbagil.py), na mesma ordem.

WITH base AS (
    SELECT *
    FROM {{ ref('bbagil_extrato_transacoes') }}
),

-- 1. Corte temporal (Ciclo 1 do PNAB)
corte_temporal AS (
    SELECT *
    FROM base
    WHERE valuedate <= '{{ var("bsc_pnab_data_corte") }}'::date
),

-- 2. Beneficiario valido e sem subtransacoes (as com subtransacoes sao
--    tratadas via bbagil_subtransacoes_filtrado, para nao contar 2x)
beneficiario_valido AS (
    SELECT *
    FROM corte_temporal
    WHERE beneficiarydocumentid <> '0'
      AND subtransactionquantity = 0
),

-- 3. Remove transferencias para o proprio Banco do Brasil
sem_bb AS (
    SELECT *
    FROM beneficiario_valido
    WHERE beneficiarydocumentid <> '{{ var("bsc_pnab_bb_documento") }}'
),

-- 4. Remove transacoes internas / impostos
sem_ruido_interno AS (
    SELECT *
    FROM sem_bb
    WHERE descriptionname NOT IN (
        'BB-APLIC C.PRZ-APL.AUT',
        'Resgate Automatico',
        'Impostos',
        'ORDEM BANC CANCELADA',
        'Ordem Bancaria',
        'Resgate BB Fix',
        'CREDITO CONVENIO',
        'Estorno Resgate Automatico'
    )
),

-- 5. Remove devolucoes de saldo ao Fundo Nacional de Cultura
sem_fnc AS (
    SELECT *
    FROM sem_ruido_interno
    WHERE beneficiarydocumentid <> '{{ var("bsc_pnab_fnc_cnpj") }}'
),

-- 6. Remove pares credito/debito (estorno no mesmo dia ou devolucao futura
--    de um debito anterior) -- mesma chave (ente, beneficiario, valor)
--    aparecendo dos dois lados (D e C) e descartada por inteiro.
sem_pares_credito_debito AS (
    SELECT s.*
    FROM sem_fnc s
    WHERE NOT EXISTS (
        SELECT 1
        FROM sem_fnc o
        WHERE o.ente = s.ente
          AND o.beneficiarydocumentid = s.beneficiarydocumentid
          AND o.value = s.value
          AND o.creditdebitindicator <> s.creditdebitindicator
    )
),

-- 7. Mantem apenas debitos (creditos remanescentes tambem sao descartados)
apenas_debito AS (
    SELECT *
    FROM sem_pares_credito_debito
    WHERE creditdebitindicator = 'D'
),

-- 8. Remove repasses entre entes publicos (nome comecando com
--    MUNICIPIO/ESTADO/FUNDO/SECRETARIA/SEFAZ)
sem_repasse_publico AS (
    SELECT *
    FROM apenas_debito
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
    value AS valor_pago
FROM sem_repasse_publico
