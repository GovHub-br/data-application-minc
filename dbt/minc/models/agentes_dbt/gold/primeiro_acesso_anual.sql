{{ config(materialized='table') }}

-- Gold — a série que responde às perguntas 1, 2.1 e 2.2 da Meta 5:
--
--   1.   percentual ANUAL de agentes de primeiro acesso, considerando
--        LAB, LPG, PNAB e Lei Rouanet  -> filtre programa_fomento = 'TODOS'
--   2.1  novos entrantes da Rouanet ao longo dos anos
--        -> filtre programa_fomento = 'ROUANET'
--   2.2  novos entrantes do fomento direto
--        -> filtre programa_fomento IN ('LAB','LPG','PNAB')
--
-- Grão: 1 linha por (ano × mecanismo), mais a linha de roll-up
-- programa_fomento = 'TODOS'.
--
-- COMO LER CADA COLUNA
--   agentes_atendidos  quantos agentes distintos receberam recurso daquele
--                      mecanismo naquele ano (denominador)
--   novos_entrantes    destes, quantos tiveram ali o primeiro acesso a
--                      QUALQUER fomento federal observável (numerador)
--   percentual_primeiro_acesso  = novos / atendidos
--
-- No roll-up 'TODOS' um agente atendido por dois mecanismos no mesmo ano
-- conta UMA vez (COUNT DISTINCT sobre o documento), então a linha 'TODOS'
-- não é a soma das linhas por mecanismo — é a população real do ano.
--
-- ANO CENSURADO: o BB Ágil começa em 2023, então em 2023 todo agente de
-- LAB/LPG/PNAB sem passado na Rouanet aparece como estreante por construção,
-- não por medição. A coluna ano_censurado marca essas linhas. Elas não são
-- lixo — são o teto do que se pode afirmar — mas publicar o percentual de um
-- ano censurado como se fosse resultado é o erro clássico aqui.
--
-- O NÚMERO É TETO, NÃO ESTIMATIVA PONTUAL: falta FSA (planilha Ancine, fora
-- do banco) e falta a LAB I emergencial de 2020-2021 (não existe em nenhuma
-- fonte). Toda fonte ausente só pode transformar veterano em falso novo
-- entrante, nunca o contrário, então o percentual real é <= o publicado.

WITH atendidos_ano_mecanismo AS (
    SELECT DISTINCT
        ano_evento AS ano,
        programa_fomento,
        beneficiario_documento
    FROM {{ ref('eventos_fomento') }}
),

-- porta de entrada: uma linha por agente, o mecanismo e o ano em que ele
-- acessou o fomento federal pela primeira vez
entradas AS (
    SELECT
        beneficiario_documento,
        programa_fomento,
        ano_primeiro_acesso_geral AS ano
    FROM {{ ref('primeiro_acesso_agentes') }}
    WHERE eh_porta_de_entrada
),

por_mecanismo AS (
    SELECT
        a.ano,
        a.programa_fomento,
        COUNT(DISTINCT a.beneficiario_documento) AS agentes_atendidos,
        COUNT(DISTINCT e.beneficiario_documento) AS novos_entrantes
    FROM atendidos_ano_mecanismo a
    LEFT JOIN entradas e
        ON a.beneficiario_documento = e.beneficiario_documento
        AND a.programa_fomento = e.programa_fomento
        AND a.ano = e.ano
    GROUP BY a.ano, a.programa_fomento
),

-- roll-up: o agente conta uma vez no ano, independente de quantos
-- mecanismos o atenderam
total_geral AS (
    SELECT
        a.ano,
        'TODOS' AS programa_fomento,
        COUNT(DISTINCT a.beneficiario_documento) AS agentes_atendidos,
        COUNT(DISTINCT e.beneficiario_documento) AS novos_entrantes
    FROM atendidos_ano_mecanismo a
    LEFT JOIN entradas e
        ON a.beneficiario_documento = e.beneficiario_documento
        AND a.ano = e.ano
    GROUP BY a.ano
),

consolidado AS (
    SELECT * FROM por_mecanismo
    UNION ALL
    SELECT * FROM total_geral
),

-- último ano com dado: serve para marcar o ano corrente como parcial. Um ano
-- que ainda não terminou tem menos gente nova acumulada e por isso puxa o
-- percentual para baixo — a queda não é achado, é calendário.
limite AS (
    SELECT MAX(ano_evento) AS ano_max FROM {{ ref('eventos_fomento') }}
)

SELECT
    c.ano,
    c.programa_fomento,
    c.agentes_atendidos,
    c.novos_entrantes,
    c.agentes_atendidos - c.novos_entrantes AS ja_contemplados_antes,
    ROUND(c.novos_entrantes::NUMERIC / NULLIF(c.agentes_atendidos, 0) * 100, 2)
        AS percentual_primeiro_acesso,
    CASE
        WHEN c.programa_fomento = 'ROUANET' THEN c.ano <= 1993
        -- no roll-up, 2023 também é censurado: é o primeiro ano em que o BB
        -- Ágil enxerga qualquer coisa, e LAB/LPG/PNAB dominam o volume dali
        -- em diante. Sem essa marca, o 93% de 2023 seria lido como achado
        -- quando é o mesmo artefato de janela dos mecanismos individuais.
        WHEN c.programa_fomento = 'TODOS' THEN c.ano <= 1993 OR c.ano = 2023
        ELSE c.ano <= 2023
    END AS ano_censurado,
    (c.ano = l.ano_max) AS ano_parcial
FROM consolidado c
CROSS JOIN limite l
ORDER BY c.programa_fomento, c.ano
