{{ config(materialized='table') }}

-- Camada Gold — Meta 5 Fase 3: primeiro acesso (autodeclarado/inferido)
-- cruzado com contemplação em edital. Identificadores de contemplados
-- vêm de identificadores_contemplados (views/) — ver esse modelo pra
-- notas de cobertura e o motivo do match dinâmico de coluna (NBSP).
--
-- ATENÇÃO — CPF mascarado dos proponentes LPG: a base de proponentes
-- (lpg_agentes_pf/coletivos) traz o CPF anonimizado no formato
-- "***.NNN.NNN-**" (apenas os 6 dígitos centrais visíveis), enquanto
-- lpg_contemplados traz o CPF completo. Um match exato (dígito a dígito)
-- nunca ocorre para esses casos, gerando falso negativo sistemático de
-- 'contemplado' para LPG. Por isso, quando o identificador do proponente
-- vier mascarado, o JOIN usa um match parcial pelo "miolo" do CPF
-- (posições 4-9, os mesmos 6 dígitos centrais expostos pela máscara).
-- Esse match parcial tem risco de colisão entre CPFs com miolo igual.

WITH todos_contemplados AS (
    SELECT id_normalizado, programa_fomento
    FROM {{ ref('identificadores_contemplados') }}
),

-- Miolo (6 dígitos centrais) do CPF, usado para casar com identificadores
-- mascarados de proponentes ("***.NNN.NNN-**"). Só faz sentido para CPF
-- (11 dígitos); CNPJ (14 dígitos) não é mascarado na base de proponentes.
todos_contemplados_miolo AS (
    SELECT
        id_normalizado,
        programa_fomento,
        SUBSTRING(id_normalizado FROM 4 FOR 6) AS miolo_cpf
    FROM todos_contemplados
    WHERE LENGTH(id_normalizado) = 11
),

-- Base: perfil_acesso_fomento tem 1 linha por (identificador × programa),
-- preservando a granularidade por programa para o JOIN de contemplação
perfil_base AS (
    SELECT
        identificador_unico,
        programa_fomento,
        CASE
            WHEN perfil_acesso_fomento IN (
                'Confirmado - Primeira Vez',
                'Inferido - Primeira Vez (Estreante na base)'
            ) THEN 'Sim'
            WHEN perfil_acesso_fomento IN (
                'Confirmado - Veterano',
                'Inferido - Veterano (Possui histórico)'
            ) THEN 'Não'
            ELSE 'Não sabe/Não informou'
        END AS categoria_primeiro_acesso,
        CASE
            WHEN perfil_acesso_fomento LIKE 'Confirmado%' THEN 'Confirmado'
            WHEN perfil_acesso_fomento LIKE 'Inferido%'   THEN 'Inferido'
            ELSE 'Não Informado'
        END AS status_dado
    FROM {{ ref('perfil_acesso_fomento') }}
),

perfil_com_contemplado AS (
    SELECT
        pb.identificador_unico,
        pb.programa_fomento,
        pb.categoria_primeiro_acesso,
        pb.status_dado,
        CASE
            WHEN tc.id_normalizado IS NOT NULL OR tcm.id_normalizado IS NOT NULL THEN 'sim'
            ELSE 'não'
        END AS contemplado
    FROM perfil_base pb
    LEFT JOIN todos_contemplados tc
        ON REGEXP_REPLACE(pb.identificador_unico, '[^0-9]', '', 'g') = tc.id_normalizado
        AND pb.programa_fomento = tc.programa_fomento
    LEFT JOIN todos_contemplados_miolo tcm
        ON pb.identificador_unico LIKE '%*%'
        AND LENGTH(REGEXP_REPLACE(pb.identificador_unico, '[^0-9]', '', 'g')) = 6
        AND REGEXP_REPLACE(pb.identificador_unico, '[^0-9]', '', 'g') = tcm.miolo_cpf
        AND pb.programa_fomento = tcm.programa_fomento
)

SELECT
    programa_fomento,
    categoria_primeiro_acesso,
    contemplado,
    COUNT(DISTINCT identificador_unico)                                                          AS total_proponentes,
    COUNT(DISTINCT CASE WHEN status_dado = 'Confirmado' THEN identificador_unico END)           AS total_campo_preenchido,
    COUNT(DISTINCT CASE WHEN status_dado = 'Inferido'   THEN identificador_unico END)           AS total_inferido,
    ROUND(
        COUNT(DISTINCT identificador_unico)::NUMERIC
        / SUM(COUNT(DISTINCT identificador_unico)) OVER (PARTITION BY programa_fomento, contemplado)
        * 100, 2
    ) AS percentual
FROM perfil_com_contemplado
GROUP BY programa_fomento, categoria_primeiro_acesso, contemplado
ORDER BY programa_fomento, contemplado DESC, total_proponentes DESC
