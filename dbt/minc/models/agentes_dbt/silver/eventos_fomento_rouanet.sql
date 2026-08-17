{{ config(materialized='table') }}

-- Silver — um evento por captação real de recurso na Lei Rouanet, no mesmo
-- formato dos pagamentos do BB Ágil, para entrar na espinha unificada
-- (eventos_fomento) que a Meta 5 usa para datar o primeiro acesso.
--
-- POR QUE CAPTAÇÃO E NÃO APROVAÇÃO: o lado do fomento direto (LPG/PNAB/LAB)
-- mede dinheiro que efetivamente saiu da conta para o agente. Para a Rouanet
-- o equivalente é o recurso efetivamente captado junto ao incentivador, não a
-- publicação da portaria de aprovação — projeto aprovado que nunca captou não
-- deu acesso a recurso nenhum ao proponente (e é o desfecho de boa parte da
-- base: veja as situações "arquivado ... sem captação"). Misturar aprovação
-- com pagamento tornaria a comparação entre mecanismos incoerente.
--
-- LIGAÇÃO ENTRE AS DUAS TABELAS: sac__captacao tem o dinheiro e a data mas não
-- tem o proponente (``cgccpfmecena`` é o incentivador que aportou, não quem
-- recebeu). O proponente está em sac__tbapiprojetorouanet.nrcnpjcpf. A chave é
-- o PRONAC: ``anoprojeto || sequencial`` de um lado, ``nrpronac`` do outro —
-- casa 667.461 de 668.365 recibos (99,86%). Não use ``captacao.idprojeto``:
-- ele é 0 na maioria das linhas. sac__projetos, que seria a ponte natural,
-- existe no schema mas foi carregada com 0 linhas.
--
-- JANELA: recibos de 1993 em diante — praticamente toda a vigência da lei
-- (1991). Diferente do BB Ágil (2023+), a Rouanet aqui quase não tem censura
-- à esquerda, e é por isso que ela é a fonte que mais consegue desmentir um
-- falso "novo entrante" do fomento direto.

WITH projeto_proponente AS (
    SELECT
        TRIM(nrpronac) AS pronac,
        -- <= 11 dígitos é CPF (PF), acima é CNPJ. O LPAD é obrigatório: a base
        -- traz documentos sem o zero à esquerda (10 e 13 dígitos), e o lado
        -- bancário sempre normaliza com LPAD — sem isso o mesmo agente vira
        -- duas pessoas diferentes no cruzamento entre mecanismos.
        CASE
            WHEN LENGTH(REGEXP_REPLACE(TRIM(nrcnpjcpf), '[^0-9]', '', 'g')) <= 11
                THEN LPAD(REGEXP_REPLACE(TRIM(nrcnpjcpf), '[^0-9]', '', 'g'), 11, '0')
            ELSE LPAD(REGEXP_REPLACE(TRIM(nrcnpjcpf), '[^0-9]', '', 'g'), 14, '0')
        END AS beneficiario_documento,
        NULLIF(TRIM(nmproponente), '') AS beneficiario_nome
    FROM {{ source('salic', 'sac__tbapiprojetorouanet') }}
    WHERE nrpronac IS NOT NULL
      AND nrcnpjcpf IS NOT NULL
      AND LENGTH(REGEXP_REPLACE(TRIM(nrcnpjcpf), '[^0-9]', '', 'g')) BETWEEN 8 AND 14
),

recibos AS (
    SELECT
        TRIM(anoprojeto) || TRIM(sequencial) AS pronac,
        dtrecibo::DATE AS data_evento,
        captacaoreal::NUMERIC AS valor
    FROM {{ source('salic', 'sac__captacao') }}
    WHERE dtrecibo IS NOT NULL
      -- as duas colunas são TEXT na bronze; o formato já foi verificado como
      -- ISO/numérico em 100% das linhas, mas o guarda fica para a ingestão
      -- não quebrar o modelo silenciosamente se isso mudar
      AND dtrecibo ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
      AND captacaoreal ~ '^-?[0-9]+(\.[0-9]+)?$'
      AND captacaoreal::NUMERIC > 0
)

SELECT
    p.beneficiario_documento,
    p.beneficiario_nome,
    'ROUANET' AS programa_fomento,
    r.data_evento,
    r.valor,
    r.pronac
FROM recibos r
INNER JOIN projeto_proponente p ON r.pronac = p.pronac
WHERE p.beneficiario_documento IS NOT NULL
  -- descarta documentos degenerados (só zeros) que não identificam ninguém
  AND p.beneficiario_documento !~ '^0+$'
