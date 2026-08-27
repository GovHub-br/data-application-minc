{{ config(materialized='table') }}

-- Gold — o mesmo indicador de novos entrantes de primeiro_acesso_anual, mas
-- restrito a quem aparece nas LISTAS OFICIAIS DE CONTEMPLADOS dos editais
-- (LPG e PNAB), em vez de a todo agente visto recebendo dinheiro. Serve para
-- a leitura "dos contemplados de edital, quantos eram novos", e para expor a
-- cobertura entre as duas populações.
--
-- Grão: 1 linha por (programa × categoria de primeiro acesso).
--
-- ATENÇÃO À COBERTURA: a interseção entre lista de contemplados e extrato
-- bancário é parcial — em torno de 46% na LPG e 89% no PNAB. A causa foi
-- medida e NÃO é atraso de pagamento nem filtro agressivo: o BB Ágil só
-- enxerga contas administradas pelo Banco do Brasil, e o município muitas
-- vezes declara ter pago um contemplado cujo pagamento não passou por lá.
-- Logo esta tabela descreve bem a fatia coberta, e não deve ser lida como
-- retrato completo da política. Contemplado sem evento observável fica de
-- fora — não há fallback autodeclarado (a base perfil_acesso_fomento tem um
-- mapeamento Sim/Não que não foi possível confirmar contra o formulário
-- original, e inverter "novo entrante" seria pior que não responder).
--
-- A classificação vem de primeiro_acesso_agentes, então já é CROSS-MECANISMO
-- e já considera a Rouanet desde 1993 — um contemplado da LPG que captou
-- Rouanet em 2010 aparece aqui corretamente como "Não".

WITH contemplados AS (
    SELECT DISTINCT id_contemplado, programa_fomento
    FROM {{ ref('identificadores_contemplados') }}
),

acesso_contemplado AS (
    SELECT
        pa.programa_fomento,
        pa.id_beneficiario,
        pa.categoria_primeiro_acesso
    FROM {{ ref('primeiro_acesso_agentes') }} pa
    INNER JOIN contemplados c
        ON pa.id_beneficiario = c.id_contemplado
        AND pa.programa_fomento = c.programa_fomento
)

SELECT
    programa_fomento,
    categoria_primeiro_acesso,
    COUNT(DISTINCT id_beneficiario) AS total_proponentes,
    ROUND(
        COUNT(DISTINCT id_beneficiario)::NUMERIC
        / SUM(COUNT(DISTINCT id_beneficiario)) OVER (PARTITION BY programa_fomento)
        * 100, 2
    ) AS percentual
FROM acesso_contemplado
GROUP BY programa_fomento, categoria_primeiro_acesso
ORDER BY programa_fomento, total_proponentes DESC
