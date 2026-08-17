{{ config(materialized='view') }}

-- Views — unifica identificadores (CPF/CNPJ normalizados) de contemplados
-- LPG + PNAB, para reuso em qualquer gold que precise cruzar com
-- contemplação (primeiro_acesso_contemplados, Fase 3;
-- primeiro_acesso_contemplados_bancario, Fase 4).
--
-- COLUNAS "FANTASMA": a ingestão dinâmica de planilhas (extracao_planilhas.py)
-- não normaliza espaços nem caracteres invisíveis nos nomes de coluna, então
-- o mesmo cabeçalho em arquivos diferentes vira colunas distintas no
-- Postgres, cada uma com parte dos dados. O que existe hoje, de fato:
--   lpg_contemplados                  "cpf ou cnpj", "cpf ou cnpj<NBSP>", "cnpj<NBSP>"
--   raw_pnab_lista_contemplados_pncv  "cpf", "cnpj", "cnpj<SP>", "cpf/ cnpj"
--   raw_pnab_lista_contemplados_geral "cnpj", "cpf ou cnpj<NBSP>"
-- (<NBSP> = U+00A0, espaço não separável — não é o espaço comum.)
--
-- Por isso a resolução é dinâmica via information_schema, e não uma lista
-- fixa de nomes. O padrão anterior, ILIKE '%cpf%cnpj%', exigia "cpf" ANTES de
-- "cnpj" na mesma string: não casava "cnpj" sozinho nem "cpf/ cnpj", e
-- deixava de fora ~2.100 contemplados do PNAB (a maioria deles COM pagamento
-- comprovado no extrato bancário) e as colunas só-CNPJ da LPG.
--
-- Colunas "cpf (anonimizado)" são excluídas de propósito: trazem
-- ***XXXXXX** em vez do documento, e não servem para cruzar com nada.
--
-- NORMALIZAÇÃO: só dígitos + LPAD para 11 (CPF) ou 14 (CNPJ). O LPAD é
-- obrigatório porque o lado bancário sempre normaliza assim; sem ele um CPF
-- que chegou com 10 dígitos (sem o zero à esquerda) nunca casa.

{% set fontes = [
    ('lpg_contemplados', 'LPG'),
    ('raw_pnab_lista_contemplados_pncv', 'PNAB'),
    ('raw_pnab_lista_contemplados_geral', 'PNAB')
] %}

{# lista plana de (tabela, programa, coluna) para gerar um UNION ALL simples #}
{% set pares = [] %}
{% for tabela, programa in fontes %}
    {% set src = source('transferegov_fundo_a_fundo', tabela) %}
    {% set consulta %}
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{{ src.schema }}'
          AND table_name = '{{ src.identifier }}'
          AND (column_name ILIKE '%cpf%' OR column_name ILIKE '%cnpj%')
          AND column_name NOT ILIKE '%anonimizado%'
        ORDER BY column_name
    {% endset %}
    {% if execute %}
        {% for col in run_query(consulta).columns[0].values() %}
            {% do pares.append((tabela, programa, col)) %}
        {% endfor %}
    {% endif %}
{% endfor %}

WITH bruto AS (
    {% for tabela, programa, col in pares %}
    SELECT
        NULLIF(TRIM("{{ col }}"), '') AS doc_bruto,
        '{{ programa }}' AS programa_fomento
    FROM {{ source('transferegov_fundo_a_fundo', tabela) }}
    {%- if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
),

limpo AS (
    SELECT
        REGEXP_REPLACE(doc_bruto, '[^0-9]', '', 'g') AS digitos,
        programa_fomento
    FROM bruto
    WHERE doc_bruto IS NOT NULL
      -- linhas de cabeçalho repetido e placeholders de leitura de planilha
      AND LOWER(doc_bruto) NOT IN ('nan', 'none', 'null', 'cpf', 'cnpj', 'cpf ou cnpj', 'cpf/ cnpj')
)

SELECT DISTINCT
    CASE
        WHEN LENGTH(digitos) <= 11 THEN LPAD(digitos, 11, '0')
        ELSE LPAD(digitos, 14, '0')
    END AS id_normalizado,
    programa_fomento
FROM limpo
-- 9 dígitos é o menor CPF plausível já visto sem zeros à esquerda; abaixo
-- disso é lixo de parsing (número de ordem, ano, célula vazia lida como 0)
WHERE LENGTH(digitos) BETWEEN 9 AND 14
  AND digitos !~ '^0+$'
