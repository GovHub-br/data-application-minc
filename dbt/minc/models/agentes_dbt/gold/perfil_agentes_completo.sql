{{ config(materialized='table') }}

WITH perfil_base AS (
    SELECT
        id_agente,
        tipo_proponente,
        programa_fomento,
        historico_acesso_limpo,
        sequencia_fomento,
        perfil_acesso_fomento AS perfil_original
    FROM {{ ref('perfil_acesso_fomento') }}
),

-- Detecta veterania multi-programa: presença em > 1 programa confirma veterano independente
-- da resposta declarada
contagem_programas AS (
    SELECT
        id_agente,
        COUNT(DISTINCT programa_fomento) AS qtd_programas
    FROM perfil_base
    GROUP BY id_agente
),

-- Registro canônico: primeiro programa de cada proponente (sequencia_fomento = 1)
-- Proxy temporal: LPG < PNAB alfabeticamente, refletindo a ordem de ingresso na base
primeiro_registro AS (
    SELECT DISTINCT ON (id_agente)
        id_agente,
        tipo_proponente,
        programa_fomento,
        historico_acesso_limpo,
        perfil_original
    FROM perfil_base
    ORDER BY id_agente, sequencia_fomento ASC
),

-- historico_acesso_bruto vem direto da fonte bronze, alinhado ao primeiro programa
bruto AS (
    SELECT DISTINCT ON (id_agente)
        id_agente,
        historico_acesso_bruto
    FROM {{ ref('identificadores_agentes') }}
    ORDER BY id_agente, programa_fomento ASC
),

consolidado AS (
    SELECT
        pr.id_agente,
        pr.tipo_proponente,
        pr.programa_fomento,
        b.historico_acesso_bruto,
        pr.historico_acesso_limpo,
        pr.perfil_original,
        c.qtd_programas
    FROM primeiro_registro pr
    LEFT JOIN bruto b
        ON pr.id_agente = b.id_agente
    LEFT JOIN contagem_programas c
        ON pr.id_agente = c.id_agente
)

SELECT
    id_agente,
    tipo_proponente,
    programa_fomento,
    historico_acesso_bruto,

    -- status_origem: 'Inferido' quando a veterania vem da presença multi-programa e
    -- não de uma resposta explícita; 'Confirmado' quando o proponente respondeu sim/não
    CASE
        WHEN qtd_programas > 1 AND perfil_original != 'Confirmado - Veterano'
            THEN 'Inferido'
        WHEN perfil_original LIKE 'Confirmado%'
            THEN 'Confirmado'
        WHEN perfil_original LIKE 'Inferido%'
            THEN 'Inferido'
        ELSE 'Não Informado'
    END AS status_origem,

    -- perfil_classificacao: classificação final de veterania, consolidada em 1 linha
    CASE
        WHEN qtd_programas > 1 AND perfil_original = 'Confirmado - Veterano'
            THEN 'Veterano'
        WHEN qtd_programas > 1
            THEN 'Veterano (Multi-Programa)'
        WHEN perfil_original = 'Confirmado - Primeira Vez'
            THEN 'Primeira Vez'
        WHEN perfil_original = 'Confirmado - Veterano'
            THEN 'Veterano'
        WHEN perfil_original = 'Inferido - Primeira Vez (Estreante na base)'
            THEN 'Provável Primeira Vez'
        WHEN perfil_original = 'Inferido - Veterano (Possui histórico)'
            THEN 'Provável Veterano'
        ELSE 'Indeterminado'
    END AS perfil_classificacao

FROM consolidado
