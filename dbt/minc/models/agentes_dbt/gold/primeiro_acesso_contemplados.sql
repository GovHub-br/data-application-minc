{{ config(materialized='table') }}

-- Camada Gold — Meta 5 Fase 3: primeiro acesso (autodeclarado/inferido)
-- cruzado com contemplação em edital. Identificadores de contemplados
-- vêm de identificadores_contemplados (views/) — ver esse modelo pra
-- notas de cobertura e o motivo do match dinâmico de coluna (NBSP).
--
-- ATENÇÃO — CPF mascarado dos proponentes LPG: a base de proponentes
-- (lpg_agentes_pf/coletivos) traz o CPF anonimizado no formato
-- "***.NNN.NNN-**" (apenas os 6 dígitos centrais visíveis), enquanto
-- planilha_contemplados_lpg traz o CPF completo. Um match exato (dígito a
-- dígito) nunca ocorre para esses casos, gerando falso negativo sistemático de
-- 'contemplado' para LPG. Por isso, quando o identificador do proponente
-- vier mascarado, o JOIN usa um match parcial pelo "miolo" do CPF
-- (posições 4-9, os mesmos 6 dígitos centrais expostos pela máscara).
-- Esse match parcial tem risco de colisão entre CPFs com miolo igual.
--
-- MUDANÇA DE NÚMERO CONHECIDA (canonização): antes do pseudônimo, o JOIN
-- exato comparava os dígitos crus do proponente contra o documento do
-- contemplado JÁ com LPAD. Documento do proponente que chegou sem o zero à
-- esquerda (10 dígitos num CPF, 12-13 num CNPJ) portanto nunca casava. Agora os
-- dois lados passam por documento_canonico antes do hash, e esses casos
-- passam a casar. O efeito é monotônico — só pode ACRESCENTAR
-- contemplado = 'sim', nunca remover — e é correção de um falso negativo, não
-- regressão. Espere este modelo divergir do valor anterior para cima.
--
-- COMO ISSO SOBREVIVE AO PSEUDÔNIMO: hash não preserva substring, então o
-- recorte do miolo não pode mais ser feito aqui. Ele é feito ANTES do hash nos
-- dois lados — id_agente_miolo na bronze de planilha (só quando o documento
-- veio mascarado e sobraram exatamente 6 dígitos) e id_contemplado_miolo em
-- identificadores_contemplados (só para CPF, 11 dígitos) — e aqui o JOIN
-- apenas casa hash com hash. As condições que antes eram testadas neste
-- modelo (LIKE '%*%' e comprimento 6) já estão embutidas no fato de
-- id_agente_miolo ser NULL fora desse caso, e NULL não casa com nada.

WITH todos_contemplados AS (
    SELECT id_contemplado, programa_fomento
    FROM {{ ref('identificadores_contemplados') }}
),

-- Miolo (6 dígitos centrais) do CPF, usado para casar com identificadores
-- mascarados de proponentes ("***.NNN.NNN-**"). Só faz sentido para CPF;
-- CNPJ não é mascarado na base de proponentes, e para ele o recorte já vem
-- NULL de identificadores_contemplados.
todos_contemplados_miolo AS (
    SELECT
        id_contemplado_miolo,
        programa_fomento
    FROM {{ ref('identificadores_contemplados') }}
    WHERE id_contemplado_miolo IS NOT NULL
),

-- Base: perfil_acesso_fomento tem 1 linha por (identificador × programa),
-- preservando a granularidade por programa para o JOIN de contemplação
perfil_base AS (
    SELECT
        id_agente,
        id_agente_miolo,
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
        pb.id_agente,
        pb.programa_fomento,
        pb.categoria_primeiro_acesso,
        pb.status_dado,
        CASE
            WHEN tc.id_contemplado IS NOT NULL
              OR tcm.id_contemplado_miolo IS NOT NULL THEN 'sim'
            ELSE 'não'
        END AS contemplado
    FROM perfil_base pb
    LEFT JOIN todos_contemplados tc
        ON pb.id_agente = tc.id_contemplado
        AND pb.programa_fomento = tc.programa_fomento
    LEFT JOIN todos_contemplados_miolo tcm
        ON pb.id_agente_miolo = tcm.id_contemplado_miolo
        AND pb.programa_fomento = tcm.programa_fomento
)

SELECT
    programa_fomento,
    categoria_primeiro_acesso,
    contemplado,
    COUNT(DISTINCT id_agente)                                                          AS total_proponentes,
    COUNT(DISTINCT CASE WHEN status_dado = 'Confirmado' THEN id_agente END)           AS total_campo_preenchido,
    COUNT(DISTINCT CASE WHEN status_dado = 'Inferido'   THEN id_agente END)           AS total_inferido,
    ROUND(
        COUNT(DISTINCT id_agente)::NUMERIC
        / SUM(COUNT(DISTINCT id_agente)) OVER (PARTITION BY programa_fomento, contemplado)
        * 100, 2
    ) AS percentual
FROM perfil_com_contemplado
GROUP BY programa_fomento, categoria_primeiro_acesso, contemplado
ORDER BY programa_fomento, contemplado DESC, total_proponentes DESC
