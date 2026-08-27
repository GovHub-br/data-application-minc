{{ config(
    materialized='incremental',
    unique_key='id_agente'
) }}

-- Bronze — proponentes pessoa física da PNAB (ciclo 1).
--
-- O documento não é materializado. O que sai daqui é o pseudônimo
-- sha256(salt || documento canônico) — ver macros/hash_documento.sql.
--
-- Documento MASCARADO ('***.NNN.NNN-**', como a base de proponentes da LPG
-- entrega o CPF) não passa por documento_canonico: os 6 dígitos visíveis
-- virariam '00000NNNNNN', um CPF de aparência legítima que colidiria com o CPF
-- real de mesmo valor. Ele é hasheado como chega — o que preserva a
-- cardinalidade e mantém o não-casamento com documentos completos que este
-- pipeline sempre teve. Para esses, o casamento possível é pelo miolo, em
-- id_agente_miolo, que sustenta o fallback de primeiro_acesso_contemplados.

WITH base AS (
    SELECT
        LOWER(TRIM("nº do cpf")) AS documento_bruto,
        LOWER(TRIM("já acessou recursos públicos do fomento à cultura nos últim")) AS historico_acesso_bruto
    FROM {{ source('relatorio_gestao', 'planilha_dados_pnab_ciclo_1') }}
    WHERE tabela_origem = 'pnab_pessoas'
      AND "nº do cpf" IS NOT NULL
      AND LOWER(TRIM("nº do cpf")) NOT IN ('', 'nan', 'none')
),

classificado AS (
    SELECT
        documento_bruto,
        historico_acesso_bruto,
        {{ documento_anonimizado('documento_bruto') }} AS documento_mascarado,
        {{ documento_canonico('documento_bruto') }} AS documento_canon,
        {{ normaliza_documento('documento_bruto') }} AS documento_digitos
    FROM base
),

pseudonimizado AS (
    SELECT
        CASE
            WHEN documento_mascarado THEN {{ hash_documento('documento_bruto') }}
            ELSE {{ hash_documento('documento_canon') }}
        END AS id_agente,
        -- só o mascarado tem miolo aqui: é o único caso em que o documento
        -- completo não existe e o match parcial precisa entrar
        CASE
            WHEN documento_mascarado AND LENGTH(documento_digitos) = 6
                THEN {{ hash_documento('documento_digitos') }}
        END AS id_agente_miolo,
        documento_mascarado,
        historico_acesso_bruto,
        'PNAB' AS programa_fomento
    FROM classificado
)

SELECT * FROM pseudonimizado

{% if is_incremental() %}
WHERE id_agente NOT IN (
    SELECT id_agente FROM {{ this }}
)
{% endif %}
