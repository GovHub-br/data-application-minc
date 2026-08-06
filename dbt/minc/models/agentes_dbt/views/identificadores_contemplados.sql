{{ config(materialized='view') }}

-- Views — unifica identificadores (CPF/CNPJ normalizados, só dígitos) de
-- contemplados LPG + PNAB, para reuso em qualquer gold que precise cruzar
-- com contemplação (primeiro_acesso_contemplados, Fase 3;
-- primeiro_acesso_contemplados_bancario, Fase 4).
--
-- ATENÇÃO — colunas "fantasma" em lpg_contemplados: a ingestão dinâmica de
-- planilhas (extracao_planilhas.py) não normaliza espaços/caracteres
-- invisíveis (ex.: NBSP) nos nomes de coluna, então o header "CPF ou CNPJ"
-- de arquivos diferentes pode virar colunas distintas no Postgres
-- (ex.: "cpf ou cnpj" vs. "cpf ou cnpj<NBSP>"), cada uma com parte dos
-- dados. Por isso a coluna de CPF/CNPJ é resolvida dinamicamente via
-- information_schema (qualquer coluna cujo nome contenha "cpf" e "cnpj"),
-- em vez de um nome fixo — o que tornaria a maior parte dos contemplados
-- invisível para o JOIN.
--
-- Cobertura:
--   - LPG: CPF e CNPJ completos via lpg_contemplados — cobertura total.
--   - PNAB PNCV: CPF real via raw_pnab_lista_contemplados_pncv — cobertura PF.
--   - PNAB Geral: apenas CNPJ via raw_pnab_lista_contemplados_geral — CPF
--     anonimizado (***XXXXXX**), PF da lista geral fora do PNCV não é rastreável.

{% set cpf_cnpj_cols_query %}
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{{ source('transferegov_fundo_a_fundo', 'lpg_contemplados').schema }}'
      AND table_name = '{{ source('transferegov_fundo_a_fundo', 'lpg_contemplados').identifier }}'
      AND column_name ILIKE '%cpf%cnpj%'
    ORDER BY column_name
{% endset %}
{% set cpf_cnpj_results = run_query(cpf_cnpj_cols_query) %}
{% set cpf_cnpj_cols = cpf_cnpj_results.columns[0].values() if execute else ['cpf ou cnpj'] %}

WITH contemplados_lpg_raw AS (
    SELECT
        COALESCE(
            {% for col in cpf_cnpj_cols %}
            NULLIF(LOWER(TRIM("{{ col }}")), 'nan')
            {%- if not loop.last %},
            {% endif %}
            {% endfor %}
        ) AS cpf_cnpj_bruto
    FROM {{ source('transferegov_fundo_a_fundo', 'lpg_contemplados') }}
),

contemplados_lpg AS (
    SELECT DISTINCT
        REGEXP_REPLACE(cpf_cnpj_bruto, '[^0-9]', '', 'g') AS id_normalizado,
        'LPG' AS programa_fomento
    FROM contemplados_lpg_raw
    WHERE cpf_cnpj_bruto IS NOT NULL
      AND cpf_cnpj_bruto NOT IN ('', 'cpf ou cnpj')
      AND LENGTH(REGEXP_REPLACE(cpf_cnpj_bruto, '[^0-9]', '', 'g')) >= 11
),

-- PNCV fornece CPF real dos contemplados PNAB (PF)
contemplados_pnab_pncv AS (
    SELECT DISTINCT
        REGEXP_REPLACE(TRIM(cpf), '[^0-9]', '', 'g') AS id_normalizado,
        'PNAB' AS programa_fomento
    FROM {{ source('transferegov_fundo_a_fundo', 'raw_pnab_lista_contemplados_pncv') }}
    WHERE cpf IS NOT NULL
      AND LOWER(TRIM(cpf)) NOT IN ('nan', '', 'cpf')
      AND LENGTH(REGEXP_REPLACE(TRIM(cpf), '[^0-9]', '', 'g')) = 11
),

-- Lista geral PNAB: CPF está anonimizado (***XXXXXX**) — só o CNPJ é utilizável
-- Cobertura parcial: PJ contemplada via PNAB geral sem registro no PNCV fica de fora
contemplados_pnab_geral_pj AS (
    SELECT DISTINCT
        REGEXP_REPLACE(TRIM(cnpj), '[^0-9]', '', 'g') AS id_normalizado,
        'PNAB' AS programa_fomento
    FROM {{ source('transferegov_fundo_a_fundo', 'raw_pnab_lista_contemplados_geral') }}
    WHERE cnpj IS NOT NULL
      AND LOWER(TRIM(cnpj)) NOT IN ('nan', '', 'cnpj')
      AND LENGTH(REGEXP_REPLACE(TRIM(cnpj), '[^0-9]', '', 'g')) = 14
)

SELECT id_normalizado, programa_fomento FROM contemplados_lpg
UNION
SELECT id_normalizado, programa_fomento FROM contemplados_pnab_pncv
UNION
SELECT id_normalizado, programa_fomento FROM contemplados_pnab_geral_pj
