{{ config(materialized='view') }}

-- Bronze — unifica os identificadores (CPF/CNPJ normalizados) das listas
-- oficiais de contemplados de edital, LPG + PNAB, para que
-- primeiro_acesso_contemplados possa restringir o indicador a quem foi de fato
-- selecionado.
--
-- COLUNAS "FANTASMA": a ingestão dinâmica de planilhas (extracao_planilhas.py)
-- não normaliza espaços nem caracteres invisíveis nos nomes de coluna, então
-- o mesmo cabeçalho em arquivos diferentes vira colunas distintas no
-- Postgres, cada uma com parte dos dados. O que existe hoje, de fato:
--   planilha_contemplados_lpg              "cpf ou cnpj", "cpf ou cnpj<NBSP>",
--                                          "cnpj<NBSP>"
--   planilha_contemplados_pnab_ciclo_1     "cpf", "cnpj", "cnpj<SP>",
--                                          "cpf/ cnpj", "cpf ou cnpj<NBSP>"
-- (<NBSP> = U+00A0, espaço não separável — não é o espaço comum.)
--
-- As duas listas do PNAB (geral e Cultura Viva) viraram fatias da mesma
-- tabela, separadas por `tabela_origem`. Aqui não é preciso filtrar por
-- fatia: as duas são PNAB e o resultado é DISTINCT, então varrer as colunas
-- de documento da tabela inteira produz exatamente o mesmo conjunto.
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
    ('planilha_contemplados_lpg', 'LPG'),
    ('planilha_contemplados_pnab_ciclo_1', 'PNAB')
] %}

{# lista plana de (tabela, programa, coluna) para gerar um UNION ALL simples #}
{% set pares = [] %}
{% for tabela, programa in fontes %}
    {% set src = source('relatorio_gestao', tabela) %}
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

{#
  Sem esta guarda o modelo falha da pior maneira possível quando as tabelas de
  planilha não existem (banco onde as DAGs de conformidade ainda não rodaram):
  a varredura do information_schema devolve zero colunas, o UNION ALL sai vazio
  e o modelo compila `WITH bruto AS ()` — SQL inválido, que só quebra no
  `dbt run` com um erro de sintaxe que não diz nada sobre a causa.
#}
{% if execute and pares | length == 0 %}
    {{ exceptions.raise_compiler_error(
        "identificadores_contemplados: nenhuma coluna de CPF/CNPJ encontrada em "
        ~ fontes | map(attribute=0) | join(', ') ~ ". As tabelas de contemplados "
        ~ "existem no banco alvo? Rode as DAGs de relatorio_gestao antes deste modelo."
    ) }}
{% endif %}

WITH bruto AS (
    {% for tabela, programa, col in pares %}
    SELECT
        NULLIF(TRIM("{{ col }}"), '') AS doc_bruto,
        '{{ programa }}' AS programa_fomento
    FROM {{ source('relatorio_gestao', tabela) }}
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
